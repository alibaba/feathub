/*
 * Copyright 2022 The FeatHub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.udf.aggregation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for utility functions in {@link AggFuncUtils}. */
public class AggFuncUtilsTest {
    @Test
    public void testInsertIntoSortedList() {
        // Insert an element at the start.
        List<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
        AggFuncUtils.insertIntoSortedList(list, 0, Comparator.comparingInt(x -> x));
        assertThat(list).containsExactly(0, 1, 2, 3);

        // Insert an element in the middle.
        list = new ArrayList<>(Arrays.asList(0, 2, 3));
        AggFuncUtils.insertIntoSortedList(list, 1, Comparator.comparingInt(x -> x));
        assertThat(list).containsExactly(0, 1, 2, 3);

        // Insert an element at the end.
        list = new ArrayList<>(Arrays.asList(0, 1, 2));
        AggFuncUtils.insertIntoSortedList(list, 3, Comparator.comparingInt(x -> x));
        assertThat(list).containsExactly(0, 1, 2, 3);
    }

    @Test
    public void testMergeSortedLists() {
        assertThat(
                        AggFuncUtils.mergeSortedLists(
                                Arrays.asList(1, 2),
                                Arrays.asList(3, 4),
                                Comparator.comparingInt(x -> x)))
                .containsExactly(1, 2, 3, 4);

        assertThat(
                        AggFuncUtils.mergeSortedLists(
                                Arrays.asList(1, 3),
                                Arrays.asList(2, 4),
                                Comparator.comparingInt(x -> x)))
                .containsExactly(1, 2, 3, 4);

        assertThat(
                        AggFuncUtils.mergeSortedLists(
                                Arrays.asList(1, 2),
                                Arrays.asList(2, 4),
                                Comparator.comparingInt(x -> x)))
                .containsExactly(1, 2, 2, 4);

        assertThat(
                        AggFuncUtils.mergeSortedLists(
                                Arrays.asList(1, 2),
                                Collections.emptyList(),
                                Comparator.comparingInt(x -> x)))
                .containsExactly(1, 2);

        assertThat(
                        AggFuncUtils.mergeSortedLists(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Comparator.comparingInt(Object::hashCode)))
                .isEmpty();
    }
}
