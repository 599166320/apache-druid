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
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.druid.collections.Sorter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Wraps a cursor, sorting the results
 */
abstract class BasedSorterIterator implements Iterator<ScanResultValue>
{
  protected final List<String> sortColumns;
  protected final boolean legacy;
  protected final Cursor cursor;
  protected final boolean hasTimeout;
  protected final long timeoutAt;
  protected final ScanQuery query;
  protected final SegmentId segmentId;
  protected final List<String> allColumns;
  protected final Ordering<Comparable>[] orderings;
  protected final List<BaseObjectColumnValueSelector> columnSelectors;

  BasedSorterIterator(
      List<String> sortColumns,
      boolean legacy,
      Cursor cursor,
      boolean hasTimeout,
      long timeoutAt,
      ScanQuery query,
      SegmentId segmentId,
      List<String> allColumns,
      List<String> orderByDirection,
      List<BaseObjectColumnValueSelector> columnSelectors
  )
  {
    this.sortColumns = sortColumns;
    this.legacy = legacy;
    this.cursor = cursor;
    this.hasTimeout = hasTimeout;
    this.timeoutAt = timeoutAt;
    this.query = query;
    this.segmentId = segmentId;
    this.allColumns = allColumns;
    this.columnSelectors = columnSelectors;
    orderings = new Ordering[orderByDirection.size()];
    for (int i = 0; i < orderByDirection.size(); i++) {
      orderings[i] = ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i))) ? Comparators.<Comparable>naturalNullsFirst() : Comparators.<Comparable>naturalNullsFirst().reverse();
    }
  }

  @Override
  public boolean hasNext()
  {
    return !cursor.isDone();
  }

  @Override
  public ScanResultValue next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
    }
    final Object events;
    final ScanQuery.ResultFormat resultFormat = query.getResultFormat();
    if (ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
      events = rowsToCompactedList();
    } else if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
      events = rowsToList();
    } else {
      throw new UOE("resultFormat[%s] is not supported", resultFormat.toString());
    }
    return new ScanResultValue(segmentId.toString(), allColumns, events);
  }

  protected List<List<Object>> rowsToCompactedList()
  {
    final Sorter<List<Object>> sorter = rowsToCompactedListSorter();
    for (; !cursor.isDone(); cursor.advance()) {
      final List<Object> theEvent = new ArrayList<>(allColumns.size());
      List<Comparable> sortValues = new ArrayList<>();
      for (int j = 0; j < allColumns.size(); j++) {
        Object obj = getColumnValue(j);
        theEvent.add(obj);
        if (sortColumns.contains(allColumns.get(j))) {
          sortValues.add((Comparable) obj);
        }
      }
      sorter.add(new Sorter.SorterElement<List<Object>>(theEvent, sortValues));
    }
    return Lists.newArrayList(sorter.drainElement());
  }

  protected List<Map<String, Object>> rowsToList()
  {

    final Sorter<Map<String, Object>> sorter = rowsToListSorter();

    for (; !cursor.isDone(); cursor.advance()) {
      final Map<String, Object> theEvent = new LinkedHashMap<>();
      List<Comparable> sortValues = new ArrayList<>();
      for (int j = 0; j < allColumns.size(); j++) {
        Object obj = getColumnValue(j);
        theEvent.put(allColumns.get(j), obj);
        if (sortColumns.contains(allColumns.get(j))) {
          sortValues.add((Comparable) obj);
        }
      }
      sorter.add(new Sorter.SorterElement<>(theEvent, sortValues));
    }
    return Lists.newArrayList(sorter.drainElement());
  }

  protected Object getColumnValue(int i)
  {
    final BaseObjectColumnValueSelector selector = columnSelectors.get(i);
    final Object value;

    if (legacy && allColumns.get(i).equals(ScanQueryEngine.LEGACY_TIMESTAMP_KEY)) {
      Preconditions.checkNotNull(selector);
      value = DateTimes.utc((long) selector.getObject());
    } else {
      value = selector == null ? null : selector.getObject();
    }

    return value;
  }

  protected Comparator<Sorter.SorterElement<Map<String, Object>>> rowsToListComparator()
  {
    Comparator<Sorter.SorterElement<Map<String, Object>>> comparator = new Comparator<Sorter.SorterElement<Map<String, Object>>>()
    {
      @Override
      public int compare(
          Sorter.SorterElement<Map<String, Object>> o1,
          Sorter.SorterElement<Map<String, Object>> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          int compare = orderings[i].compare(o1.getOrderByColumValues().get(i), o2.getOrderByColumValues().get(i));
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
    return comparator;
  }

  protected Comparator<Sorter.SorterElement<List<Object>>> rowsToCompactedListComparator()
  {
    Comparator<Sorter.SorterElement<List<Object>>> comparator = new Comparator<Sorter.SorterElement<List<Object>>>()
    {
      @Override
      public int compare(
          Sorter.SorterElement<List<Object>> o1,
          Sorter.SorterElement<List<Object>> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          int compare = orderings[i].compare(o1.getOrderByColumValues().get(i), o2.getOrderByColumValues().get(i));
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
    return comparator;
  }

  abstract Sorter<Map<String, Object>> rowsToListSorter();

  abstract Sorter<List<Object>> rowsToCompactedListSorter();
}
