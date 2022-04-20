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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * A RichCoFlatMapFunction that can delegate to a RichCoFlatMapFunction in either the exercise or in
 * the solution. The implementation in the exercise is tested first, and if it throws
 * MissingSolutionException, then the solution is tested instead.
 *
 * <p>This can be used to write test harness tests.
 *
 * @param <IN1> first input type
 * @param <IN2> second input type
 * @param <OUT> output type
 */
public class ComposedRichCoFlatMapFunction<IN1, IN2, OUT>
        extends RichCoFlatMapFunction<IN1, IN2, OUT> {
    private final RichCoFlatMapFunction<IN1, IN2, OUT> exercise;
    private final RichCoFlatMapFunction<IN1, IN2, OUT> solution;
    private RichCoFlatMapFunction<IN1, IN2, OUT> implementationToTest;

    public ComposedRichCoFlatMapFunction(
            RichCoFlatMapFunction<IN1, IN2, OUT> exercise,
            RichCoFlatMapFunction<IN1, IN2, OUT> solution) {

        this.exercise = exercise;
        this.solution = solution;
        this.implementationToTest = exercise;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        try {
            exercise.setRuntimeContext(this.getRuntimeContext());
            exercise.open(parameters);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                this.implementationToTest = solution;
                solution.setRuntimeContext(this.getRuntimeContext());
                solution.open(parameters);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void flatMap1(IN1 value, Collector<OUT> out) throws Exception {

        implementationToTest.flatMap1(value, out);
    }

    @Override
    public void flatMap2(IN2 value, Collector<OUT> out) throws Exception {

        implementationToTest.flatMap2(value, out);
    }
}
