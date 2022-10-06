/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.scan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.collections.QueueBasedSorter;
import org.apache.druid.collections.Sorter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class OrderByQueryRunner implements QueryRunner<ScanResultValue>
{
  protected final ScanQueryEngine engine;
  protected final Segment segment;

  public OrderByQueryRunner(ScanQueryEngine engine, Segment segment)
  {
    this.engine = engine;
    this.segment = segment;
  }

  @Override
  public Sequence<ScanResultValue> run(QueryPlus<ScanResultValue> queryPlus, ResponseContext responseContext)
  {
    Query<ScanResultValue> query = queryPlus.getQuery();
    if (!(query instanceof ScanQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
    }
    ScanQuery scanQuery = (ScanQuery) query;
    // it happens in unit tests
    final Long timeoutAt = responseContext.getTimeoutTime();
    if (timeoutAt == null || timeoutAt == 0L) {
      responseContext.putTimeoutTime(JodaUtils.MAX_INSTANT);
    }
    if (scanQuery.scanOrderByNonTime()) {
      return getOrderByQueryRunner(scanQuery).process(scanQuery, segment, responseContext, queryPlus.getQueryMetrics());
    } else {
      return new ScanQueryRunnerFactory.ScanQueryRunner(engine, segment).run(queryPlus, responseContext);
    }
  }

  protected Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    if (segment.asQueryableIndex() != null && segment.asQueryableIndex().isFromTombstone()) {
      return Sequences.empty();
    }

    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");

    final Long numScannedRows = responseContext.getRowScanCount();
    if (numScannedRows != null
        && numScannedRows >= query.getScanRowsLimit()
        && query.getTimeOrder()
                .equals(ScanQuery.Order.NONE)
        && !query.scanOrderByNonTime()) {
      return Sequences.empty();
    }
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final Long timeoutAt = responseContext.getTimeoutTime();
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<String> allColumns = new ArrayList<>();

    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      if (legacy && !query.getColumns().contains(ScanQueryEngine.LEGACY_TIMESTAMP_KEY)) {
        allColumns.add(ScanQueryEngine.LEGACY_TIMESTAMP_KEY);
      }

      // Unless we're in legacy mode, allColumns equals query.getColumns() exactly. This is nice since it makes
      // the compactedList form easier to use.
      allColumns.addAll(query.getColumns());
    } else {
      final Set<String> availableColumns = Sets.newLinkedHashSet(
          Iterables.concat(
              Collections.singleton(legacy ? ScanQueryEngine.LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
              Iterables.transform(
                  Arrays.asList(query.getVirtualColumns().getVirtualColumns()),
                  VirtualColumn::getOutputName
              ),
              adapter.getAvailableDimensions(),
              adapter.getAvailableMetrics()
          )
      );

      allColumns.addAll(availableColumns);

      if (legacy) {
        allColumns.remove(ColumnHolder.TIME_COLUMN_NAME);
      }
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final SegmentId segmentId = segment.getId();

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    // If the row count is not set, set it to 0, else do nothing.
    responseContext.addRowScanCount(0);
    return getScanOrderByResultValueSequence(
        new CursorDefinition(
            query,
            responseContext,
            legacy,
            hasTimeout,
            timeoutAt,
            adapter,
            allColumns,
            intervals,
            segmentId,
            filter,
            queryMetrics
        )
    );
  }

  protected Sequence<ScanResultValue> getScanOrderByResultValueSequence(
      CursorDefinition cursorDefinition
  )
  {
    List<String> sortColumns = cursorDefinition.query.getOrderBys()
                                    .stream()
                                    .map(orderBy -> orderBy.getColumnName())
                                    .collect(Collectors.toList());
    List<String> orderByDirection = cursorDefinition.query.getOrderBys()
                                         .stream()
                                         .map(orderBy -> orderBy.getOrder().toString())
                                         .collect(Collectors.toList());
    final int limit = Math.toIntExact(cursorDefinition.query.getScanRowsLimit());
    Sorter<Long> sorter = getSorter(
        orderByDirection,
        limit
    );

    Sequence<Cursor> cursorSequence = cursorDefinition.adapter.makeCursors(
        cursorDefinition.filter,
        cursorDefinition.intervals.get(0),
        cursorDefinition.query.getVirtualColumns(),
        Granularities.ALL,
        cursorDefinition.query.getTimeOrder().equals(ScanQuery.Order.DESCENDING) ||
        (cursorDefinition.query.getTimeOrder().equals(ScanQuery.Order.NONE) && cursorDefinition.query.isDescending()),
        cursorDefinition.queryMetrics
    );

    //Materialize the data of topKOffsetSequences in advance
    cursorSequence.map(cursor -> new TopKOffsetSequence(
        new TopKOffsetSequence.TopKOffsetIteratorMaker(
            sortColumns,
            cursorDefinition.legacy,
            cursor,
            cursorDefinition.hasTimeout,
            cursorDefinition.timeoutAt,
            cursorDefinition.query,
            cursorDefinition.segmentId,
            cursorDefinition.allColumns,
            sorter
        )
    )).forEach((s) -> {
      s.toList();
    });
    Map<Long, List<Comparable>> topKOffsetSortValueMap = Maps.newHashMapWithExpectedSize(sorter.size());
    sorter.drainOrderByColumValues().forEachRemaining(d -> topKOffsetSortValueMap.putAll(d));

    return Sequences.concat(
        cursorDefinition.adapter
            .makeCursors(
                cursorDefinition.filter,
                cursorDefinition.intervals.get(0),
                cursorDefinition.query.getVirtualColumns(),
                Granularities.ALL,
                cursorDefinition.query.getTimeOrder().equals(ScanQuery.Order.DESCENDING) ||
                (cursorDefinition.query.getTimeOrder().equals(ScanQuery.Order.NONE) && cursorDefinition.query.isDescending()),
                cursorDefinition.queryMetrics
            )
            .map(cursor -> new OrderBySequence(
                new OrderBySequence.OrderByIteratorMaker(
                    cursorDefinition.legacy,
                    cursor,
                    cursorDefinition.hasTimeout,
                    cursorDefinition.timeoutAt,
                    cursorDefinition.query,
                    cursorDefinition.segmentId,
                    cursorDefinition.allColumns,
                    cursorDefinition.responseContext,
                    topKOffsetSortValueMap,
                    sortColumns
                )
            ))
    );
  }

  @Nonnull
  private Sorter<Long> getSorter(List<String> orderByDirection, int limit)
  {

    Comparator<Sorter.SorterElement<Long>> comparator = new Comparator<Sorter.SorterElement<Long>>()
    {
      Ordering<Comparable>[] comparableOrderings = orderByDirection.stream()
                                                                   .map(d -> (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(
                                                                       d)))
                                                                             ? (Comparators.<Comparable>naturalNullsFirst())
                                                                             : (Comparators.<Comparable>naturalNullsFirst()
                                                                                           .reverse()))
                                                                   .toArray(Ordering[]::new);

      @Override
      public int compare(
          Sorter.SorterElement<Long> o1,
          Sorter.SorterElement<Long> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          int compare = comparableOrderings[i].compare(
              o1.getOrderByColumValues().get(i),
              o2.getOrderByColumValues().get(i)
          );
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };

    Sorter<Long> sorter = new QueueBasedSorter<Long>(limit, comparator);
    return sorter;
  }

  private OrderByQueryRunner getOrderByQueryRunner(ScanQuery scanQuery)
  {
    if (scanQuery.getContext().containsKey(ScanQueryConfig.CTX_KEY_QUERY_RUNNER_TYPE)) {
      if (ListBasedOrderByQueryRunner.class.getSimpleName()
                                           .equalsIgnoreCase(scanQuery.getContext()
                                                                      .get(ScanQueryConfig.CTX_KEY_QUERY_RUNNER_TYPE)
                                                                      .toString())) {
        return new ListBasedOrderByQueryRunner(engine, segment);
      } else if (TreeMultisetBasedOrderByQueryRunner.class.getSimpleName()
                                                          .equalsIgnoreCase(scanQuery.getContext()
                                                                                     .get(ScanQueryConfig.CTX_KEY_QUERY_RUNNER_TYPE)
                                                                                     .toString()
                                                          )) {
        return new TreeMultisetBasedOrderByQueryRunner(engine, segment);
      } else if (TreeSetBasedOrderByQueryRunner.class.getSimpleName()
                                                     .equalsIgnoreCase(scanQuery.getContext()
                                                                                .get(ScanQueryConfig.CTX_KEY_QUERY_RUNNER_TYPE)
                                                                                .toString()
                                                     )) {
        return new TreeSetBasedOrderByQueryRunner(engine, segment);
      }
    }
    return this;
  }

  static class CursorDefinition
  {
    final ScanQuery query;
    final ResponseContext responseContext;
    final boolean legacy;
    final boolean hasTimeout;
    final long timeoutAt;
    final StorageAdapter adapter;
    final List<String> allColumns;
    final List<Interval> intervals;
    final SegmentId segmentId;
    final Filter filter;
    final QueryMetrics<?> queryMetrics;

    CursorDefinition(
        ScanQuery query,
        ResponseContext responseContext,
        boolean legacy,
        boolean hasTimeout,
        long timeoutAt,
        StorageAdapter adapter,
        List<String> allColumns,
        List<Interval> intervals,
        SegmentId segmentId,
        Filter filter,
        QueryMetrics<?> queryMetrics
    )
    {
      this.query = query;
      this.responseContext = responseContext;
      this.legacy = legacy;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.adapter = adapter;
      this.allColumns = allColumns;
      this.intervals = intervals;
      this.segmentId = segmentId;
      this.filter = filter;
      this.queryMetrics = queryMetrics;
    }
  }
}
