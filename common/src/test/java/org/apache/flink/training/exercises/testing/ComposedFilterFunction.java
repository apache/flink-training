/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

public class ComposedFilterFunction<T> implements FilterFunction<T> {

    private final FilterFunction<T> exercise;
    private final FilterFunction<T> solution;

    public ComposedFilterFunction(FilterFunction<T> exercise, FilterFunction<T> solution) {
        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public boolean filter(T value) throws Exception {
        boolean result;

        try {
            result = exercise.filter(value);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                result = solution.filter(value);
            } else {
                throw e;
            }
        }

        return result;
    }
}
