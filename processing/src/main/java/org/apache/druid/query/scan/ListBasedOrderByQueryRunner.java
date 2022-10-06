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

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.Segment;

import java.util.List;
import java.util.stream.Collectors;

public class ListBasedOrderByQueryRunner extends OrderByQueryRunner
{

  public ListBasedOrderByQueryRunner(ScanQueryEngine engine, Segment segment)
  {
    super(engine, segment);
  }

  @Override
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

    return Sequences.concat(cursorDefinition.adapter.makeCursors(
        cursorDefinition.filter,
        cursorDefinition.intervals.get(0),
        cursorDefinition.query.getVirtualColumns(),
        Granularities.ALL,
        cursorDefinition.query.getTimeOrder().equals(ScanQuery.Order.DESCENDING) ||
        (cursorDefinition.query.getTimeOrder().equals(ScanQuery.Order.NONE) && cursorDefinition.query.isDescending()),
        cursorDefinition.queryMetrics
    ).map(cursor -> new ListBasedSorterSequence(
        new ListBasedSorterSequence.ListBasedSorterIteratorMaker(
            sortColumns,
            cursorDefinition.legacy,
            cursor,
            cursorDefinition.hasTimeout,
            cursorDefinition.timeoutAt,
            cursorDefinition.query,
            cursorDefinition.segmentId,
            cursorDefinition.allColumns,
            orderByDirection
        )
    )));
  }
}
