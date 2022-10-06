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

package org.apache.druid.collections;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ListBasedSorterTests extends SorterTests
{
  @Test
  public void singleColumnAscSort()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING");
    Comparator<Sorter.SorterElement<Integer>> comparator = getMultiColumnSorterElementComparator(orderByDirection);
    ListBasedSorter multiColumnSorter = new ListBasedSorter(comparator);
    singleColumnAscSortDatas(multiColumnSorter, new ArrayList<>());
    Iterator<Integer> it = multiColumnSorter.drainElement(5);
    List<Integer> expectedValues = ImmutableList.of(100, 1, 1, 2, 3);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }

  @Test
  public void singleColumnAscSortNaturalNullsFirst()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING");
    Comparator<Sorter.SorterElement<Integer>> comparator = getMultiColumnSorterElementComparator(orderByDirection);
    ListBasedSorter multiColumnSorter = new ListBasedSorter(comparator);
    singleColumnAscSortNaturalNullsFirstDatas(multiColumnSorter, new ArrayList<>());
    Iterator<Integer> it = multiColumnSorter.drainElement(5);
    List<Integer> expectedValues = ImmutableList.of(100, 1, 1, 2, 3);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }

  @Test
  public void multiColumnSort()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    Comparator<Sorter.SorterElement<Integer>> comparator = getMultiColumnSorterElementComparator(orderByDirection);
    ListBasedSorter multiColumnSorter = new ListBasedSorter(comparator);
    List<Integer> expectedValues = new ArrayList<>();
    multiColumnSortDatas(multiColumnSorter, expectedValues);
    Iterator<Integer> it = multiColumnSorter.drainElement(5);

    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }


  @Test
  public void multiColumnSorWithNull()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    ListBasedSorter multiColumnSorter = new ListBasedSorter(getMultiColumnSorterElementComparator(orderByDirection));
    List<Integer> expectedValues = ImmutableList.of(6, 5, 1, 7);
    multiColumnSorWithNullDatas(multiColumnSorter, new ArrayList<>());
    Iterator<Integer> it = multiColumnSorter.drainElement(4);

    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }


  @Test
  public void multiColumnSortCalssCastException()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    ListBasedSorter multiColumnSorter = new ListBasedSorter(getMultiColumnSorterElementComparator(orderByDirection));
    ISE ise = null;
    try {
      multiColumnSortCalssCastExceptionDatas(multiColumnSorter);
      multiColumnSorter.drainElement();
    }
    catch (ISE e) {
      ise = e;
    }
    Assert.assertNotNull(ise);
    Assert.assertEquals("The sorted column cannot have different types of values.", ise.getMessage());
  }
}
