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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.utils.DataGenerator;
import org.apache.flink.training.exercises.testing.ComposedPipeline;
import org.apache.flink.training.exercises.testing.ExecutablePipeline;
import org.apache.flink.training.exercises.testing.ParallelTestSource;
import org.apache.flink.training.exercises.testing.TestSink;
import org.apache.flink.training.solutions.hourlytips.HourlyTipsSolution;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class HourlyTipsTest {

    private static final int PARALLELISM = 2;

    /** This isn't necessary, but speeds up the tests. */
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testOneDriverOneTip() throws Exception {

        TaxiFare one = testFare(1, t(0), 1.0F);

        ParallelTestSource<TaxiFare> source = new ParallelTestSource<>(one);

        Tuple3<Long, Long, Float> expected = Tuple3.of(t(60).toEpochMilli(), 1L, 1.0F);

        assertThat(results(source)).containsExactly(expected);
    }

    @Test
    public void testTipsAreSummedByHour() throws Exception {
        TaxiFare oneIn1 = testFare(1, t(0), 1.0F);
        TaxiFare fiveIn1 = testFare(1, t(15), 5.0F);
        TaxiFare tenIn2 = testFare(1, t(90), 10.0F);

        ParallelTestSource<TaxiFare> source = new ParallelTestSource<>(oneIn1, fiveIn1, tenIn2);

        Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 1L, 6.0F);
        Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 1L, 10.0F);

        assertThat(results(source)).containsExactlyInAnyOrder(hour1, hour2);
    }

    @Test
    public void testMaxAcrossDrivers() throws Exception {
        TaxiFare oneFor1In1 = testFare(1, t(0), 1.0F);
        TaxiFare fiveFor1In1 = testFare(1, t(15), 5.0F);
        TaxiFare tenFor1In2 = testFare(1, t(90), 10.0F);
        TaxiFare twentyFor2In2 = testFare(2, t(90), 20.0F);
        TaxiFare zeroFor3In2 = testFare(3, t(70), 0.0F);
        TaxiFare zeroFor4In2 = testFare(4, t(70), 0.0F);
        TaxiFare oneFor4In2 = testFare(4, t(80), 1.0F);
        TaxiFare tenFor5In2 = testFare(5, t(100), 10.0F);

        ParallelTestSource<TaxiFare> source =
                new ParallelTestSource<>(
                        oneFor1In1,
                        fiveFor1In1,
                        tenFor1In2,
                        twentyFor2In2,
                        zeroFor3In2,
                        zeroFor4In2,
                        oneFor4In2,
                        tenFor5In2);

        Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 1L, 6.0F);
        Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 2L, 20.0F);

        assertThat(results(source)).containsExactlyInAnyOrder(hour1, hour2);
    }

    public Instant t(int minutes) {
        return DataGenerator.BEGINNING.plus(Duration.ofMinutes(minutes));
    }

    private TaxiFare testFare(long driverId, Instant startTime, float tip) {
        return new TaxiFare(0, 0, driverId, startTime, "", tip, 0F, 0F);
    }

    private ComposedPipeline<TaxiFare, Tuple3<Long, Long, Float>> hourlyTipsPipeline() {

        ExecutablePipeline<TaxiFare, Tuple3<Long, Long, Float>> exercise =
                (source, sink) -> new HourlyTipsExercise(source, sink).execute();

        ExecutablePipeline<TaxiFare, Tuple3<Long, Long, Float>> solution =
                (source, sink) -> new HourlyTipsSolution(source, sink).execute();

        return new ComposedPipeline<>(exercise, solution);
    }

    protected List<Tuple3<Long, Long, Float>> results(SourceFunction<TaxiFare> source)
            throws Exception {

        TestSink<Tuple3<Long, Long, Float>> sink = new TestSink<>();
        JobExecutionResult jobResult = hourlyTipsPipeline().execute(source, sink);
        return sink.getResults(jobResult);
    }
}
