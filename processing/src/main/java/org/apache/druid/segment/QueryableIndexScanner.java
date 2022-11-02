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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.collections.Sorter;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Offset;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryableIndexScanner
{

  public int[] rowIds()
  {
    return null;
  }

  public int getDicId(int rowId, String column)
  {
    return 0;
  }

  public int[] getDicId(int rowId, List<String> columns)
  {
    return null;
  }

  public List<List<Integer>> sort(List<List<Integer>> dicIdList){
    Sorter<Integer> sorter = null;
    for (List<Integer> r : dicIdList) {
      sorter.add(r);
    }
    sorter.drainElement();
    final List<List<Integer>> sortedElements = new ArrayList<>(sorter.size());
    Iterators.addAll(sortedElements, sorter.drainElement());
    return sortedElements;
  }


  public Object getDicValue(int dicId)
  {
    return null;
  }

  public void test()
  {
    QueryableIndex index = null;
    Filter filter = null;
    VirtualColumns virtualColumns = null;
    QueryMetrics<? extends Query> metrics = null;
    Granularity gran = null;
    Interval interval = null;
    long minDataTimestamp = 0;
    long maxDataTimestamp = 0;
    final Closer closer = Closer.create();
    final ColumnCache columnCache = new ColumnCache(index, closer);
    final ColumnSelectorColumnIndexSelector bitmapIndexSelector = new ColumnSelectorColumnIndexSelector(
        index.getBitmapFactoryForDimensions(),
        virtualColumns,
        columnCache
    );

    final FilterAnalysis filterAnalysis = FilterAnalysis.analyzeFilter(
        filter,
        bitmapIndexSelector,
        metrics,
        index.getNumRows()
    );

    final ImmutableBitmap filterBitmap = filterAnalysis.getPreFilterBitmap();
    final Filter postFilter = filterAnalysis.getPostFilter();
    Offset baseOffset = null;
    boolean descending = false;
    if (filterBitmap == null) {
      baseOffset = descending
                   ? new SimpleDescendingOffset(index.getNumRows())
                   : new SimpleAscendingOffset(index.getNumRows());
    } else {
      baseOffset = BitmapOffset.of(filterBitmap, descending, index.getNumRows());
    }
    final NumericColumn timestamps = (NumericColumn) columnCache.getColumn(ColumnHolder.TIME_COLUMN_NAME);
    Iterable<Interval> iterable = gran.getIterable(interval);
    if (descending) {
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }


    Interval inputInterval = null;
    final long timeStart = Math.max(interval.getStartMillis(), inputInterval.getStartMillis());
    final long timeEnd = Math.min(
        interval.getEndMillis(),
        gran.increment(inputInterval.getStartMillis())
    );

    if (descending) {
      for (; baseOffset.withinBounds(); baseOffset.increment()) {
        if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
          break;
        }
      }
    } else {
      for (; baseOffset.withinBounds(); baseOffset.increment()) {
        if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
          break;
        }
      }
    }

    final Offset offset = descending ?
                          new QueryableIndexCursorSequenceBuilder.DescendingTimestampCheckingOffset(
                              baseOffset,
                              timestamps,
                              timeStart,
                              minDataTimestamp >= timeStart
                          ) :
                          new QueryableIndexCursorSequenceBuilder.AscendingTimestampCheckingOffset(
                              baseOffset,
                              timestamps,
                              timeEnd,
                              maxDataTimestamp < timeEnd
                          );


    final Offset baseCursorOffset = baseOffset.clone();

    final ColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
        virtualColumns,
        descending,
        baseCursorOffset.getBaseReadableOffset(),
        columnCache
    );

    final DateTime myBucket = gran.toDateTime(inputInterval.getStartMillis());


    if (postFilter == null) {
      QueryableIndexCursorSequenceBuilder.QueryableIndexCursor(baseCursorOffset, columnSelectorFactory, myBucket);
    } else {
      FilteredOffset filteredOffset = new FilteredOffset(
          baseCursorOffset,
          columnSelectorFactory,
          descending,
          postFilter,
          bitmapIndexSelector
      );
      QueryableIndexCursorSequenceBuilder.QueryableIndexCursor(filteredOffset, columnSelectorFactory, myBucket);
    }

    columnSelectorFactory.getRowIdSupplier();




    RoaringBitmapFactory roaringBitmapFactory = new RoaringBitmapFactory();

    MutableBitmap mutableBitmap = new  WrappedRoaringBitmap(true);
    roaringBitmapFactory.makeImmutableBitmap(mutableBitmap);
    //同时对baseOffset和postFilter过滤，生成新的Offset




    ColumnValueSelector columnValueSelector = index.getColumnHolder("").getColumn().makeColumnValueSelector(baseCursorOffset);
    columnValueSelector.getObject();


    IntIterator ids = filterBitmap.iterator();
    //过滤的行都存储在result
    List<Integer> result = null;
    if (ids.hasNext()) {
      result = new ArrayList<>();
      while (ids.hasNext()) {
        result.add(ids.next());
      }
    } else {
      result = Collections.emptyList();
    }

    final List<String> allColumns = new ArrayList<>();
    final List<String> orderByColumns = new ArrayList<>();
    //通过offset获取个列的字典

  }
}
