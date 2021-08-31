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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.ComposedPipeline;
import org.apache.flink.training.exercises.testing.ExecutablePipeline;
import org.apache.flink.training.exercises.testing.ParallelTestSource;
import org.apache.flink.training.exercises.testing.TestSink;
import org.apache.flink.training.solutions.longrides.LongRidesSolution;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LongRidesIntegrationTest extends LongRidesTestBase {

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
    public void shortRide() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinLater = endRide(rideStarted, ONE_MINUTE_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(rideStarted, endedOneMinLater);

        assertThat(results(source)).isEmpty();
    }

    @Test
    public void shortRideOutOfOrder() throws Exception {
        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinLater = endRide(rideStarted, ONE_MINUTE_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(endedOneMinLater, rideStarted);

        assertThat(results(source)).isEmpty();
    }

    @Test
    public void multipleRides() throws Exception {
        TaxiRide longRideWithoutEnd = startRide(1, BEGINNING);
        TaxiRide twoHourRide = startRide(2, BEGINNING);
        TaxiRide otherLongRide = startRide(3, ONE_MINUTE_LATER);
        TaxiRide shortRide = startRide(4, ONE_HOUR_LATER);
        TaxiRide shortRideEnded = endRide(shortRide, TWO_HOURS_LATER);
        TaxiRide twoHourRideEnded = endRide(twoHourRide, BEGINNING);
        TaxiRide otherLongRideEnded = endRide(otherLongRide, THREE_HOURS_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(
                        longRideWithoutEnd,
                        twoHourRide,
                        otherLongRide,
                        shortRide,
                        shortRideEnded,
                        twoHourRideEnded,
                        otherLongRideEnded);

        assertThat(results(source))
                .containsExactlyInAnyOrder(longRideWithoutEnd.rideId, otherLongRide.rideId);
    }

    private static final ExecutablePipeline<TaxiRide, Long> exercise =
            (source, sink) -> new LongRidesExercise(source, sink).execute();

    private static final ExecutablePipeline<TaxiRide, Long> solution =
            (source, sink) -> new LongRidesSolution(source, sink).execute();

    protected List<Long> results(SourceFunction<TaxiRide> source) throws Exception {

        TestSink<Long> sink = new TestSink<>();
        ComposedPipeline<TaxiRide, Long> longRidesPipeline =
                new ComposedPipeline<>(exercise, solution);
        JobExecutionResult jobResult = longRidesPipeline.execute(source, sink);
        return sink.getResults(jobResult);
    }
}
