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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.collections.QueueBasedSorter;
import org.apache.druid.collections.Sorter;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.AbstractPrioritizedQueryRunnerCallable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Scan the segments in parallel, complete the sorting of each batch within each segment, and then complete the sorting of each segment level
 */
public class ScanQueryOrderBySequence extends BaseSequence<ScanResultValue, Iterator<ScanResultValue>>
{
  public ScanQueryOrderBySequence(
      QueryPlus<ScanResultValue> queryPlus,
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<ScanResultValue>> queryables,
      ResponseContext responseContext
  )
  {
    super(new ScanQueryOrderByIteratorMaker(queryPlus, queryProcessingPool, queryables, responseContext));
  }

  static class ScanQueryOrderByIteratorMaker
      implements BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>
  {
    private static final Logger log = new Logger(ScanQueryOrderByIteratorMaker.class);
    private final ScanQuery query;
    private final int priority;
    private final int limit;
    private final QueryPlus<ScanResultValue> threadSafeQueryPlus;
    private final ResponseContext responseContext;
    private final Iterable<QueryRunner<ScanResultValue>> queryables;
    private final QueryProcessingPool queryProcessingPool;

    ScanQueryOrderByIteratorMaker(
        QueryPlus<ScanResultValue> queryPlus,
        QueryProcessingPool queryProcessingPool,
        Iterable<QueryRunner<ScanResultValue>> queryables,
        ResponseContext responseContext
    )
    {
      query = (ScanQuery) queryPlus.getQuery();
      priority = QueryContexts.getPriority(query);
      threadSafeQueryPlus = queryPlus.withoutThreadUnsafeState();
      this.responseContext = responseContext;
      this.queryables = Iterables.unmodifiableIterable(queryables);
      this.queryProcessingPool = queryProcessingPool;
      if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
        limit = Integer.MAX_VALUE;
      } else {
        limit = Math.toIntExact(query.getScanRowsLimit());
      }
    }

    @Override
    public Iterator<ScanResultValue> make()
    {
      // Make it a List<> to materialize all of the values (so that it will submit everything to the executor)
      List<ListenableFuture<ScanResultValue>> futures =
          Lists.newArrayList(
              Iterables.transform(
                  queryables,
                  input -> {
                    if (input == null) {
                      throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                    }

                    return queryProcessingPool.submitRunnerTask(
                        new AbstractPrioritizedQueryRunnerCallable<ScanResultValue, ScanResultValue>(
                            priority,
                            input
                        )
                        {
                          @Override
                          public ScanResultValue call()
                          {
                            try {
                              Sequence<ScanResultValue> result = input.run(threadSafeQueryPlus, responseContext);
                              if (result == null) {
                                throw new ISE("Got a null result! Segments are missing!");
                              }
                              //Sort each batch of segment data to complete the sorting of the entire segment
                              Sorter<Object> sorter = new QueueBasedSorter<>(
                                  limit,
                                  query.getOrderByNoneTimeResultOrdering()
                              );

                              Iterator<ScanResultValue> it = result.toList().iterator();

                              List<String> columns = new ArrayList<>();
                              while (it.hasNext()) {
                                ScanResultValue next = it.next();
                                List<ScanResultValue> singleEventScanResultValues = next.toSingleEventScanResultValues();
                                for (ScanResultValue srv : singleEventScanResultValues) {
                                  columns = columns.isEmpty() ? srv.getColumns() : columns;
                                  List events = (List) (srv.getEvents());
                                  for (Object event : events) {
                                    sorter.add((List<Object>) event);
                                  }
                                }
                              }
                              final List<List<Object>> sortedElements = new ArrayList<>(sorter.size());
                              Iterators.addAll(sortedElements, sorter.drainElement());
                              List<String> finalColumns = columns;
                              return new ScanResultValue(null, finalColumns, sortedElements);
                            }
                            catch (QueryInterruptedException e) {
                              throw new RuntimeException(e);
                            }
                            catch (QueryTimeoutException e) {
                              throw e;
                            }
                            catch (Exception e) {
                              log.noStackTrace().error(e, "Exception with one of the sequences!");
                              Throwables.propagateIfPossible(e);
                              throw new RuntimeException(e);
                            }
                          }
                        });
                  }
              )
          );

      ListenableFuture<List<ScanResultValue>> future = Futures.allAsList(futures);

      try {
        //Result of merging multiple segments
        return QueryContexts.hasTimeout(query) ?
               future.get(QueryContexts.getTimeout(query), TimeUnit.MILLISECONDS).iterator() :
               future.get().iterator();
      }
      catch (InterruptedException e) {
        log.noStackTrace().warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
        //Note: canceling combinedFuture first so that it can complete with INTERRUPTED as its final state. See ChainedExecutionQueryRunnerTest.testQueryTimeout()
        GuavaUtils.cancelAll(true, future, futures);
        throw new QueryInterruptedException(e);
      }
      catch (CancellationException e) {
        throw new QueryInterruptedException(e);
      }
      catch (TimeoutException e) {
        log.warn("Query timeout, cancelling pending results for query id [%s]", query.getId());
        GuavaUtils.cancelAll(true, future, futures);
        throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
      }
      catch (ExecutionException e) {
        GuavaUtils.cancelAll(true, future, futures);
        Throwables.propagateIfPossible(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public void cleanup(Iterator<ScanResultValue> tIterator)
    {

    }

  }
}
