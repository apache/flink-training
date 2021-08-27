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

package org.apache.flink.training.exercises.ridesandfares;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.training.exercises.common.datatypes.RideAndFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.ComposedTwoInputPipeline;
import org.apache.flink.training.exercises.testing.ExecutableTwoInputPipeline;
import org.apache.flink.training.exercises.testing.ParallelTestSource;
import org.apache.flink.training.exercises.testing.TestSink;
import org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class RidesAndFaresIntegrationTest extends RidesAndFaresTestBase {

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
    public void testSeveralRidesAndFaresMixedTogether() throws Exception {

        final TaxiRide ride1 = testRide(1);
        final TaxiFare fare1 = testFare(1);

        final TaxiRide ride2 = testRide(2);
        final TaxiFare fare2 = testFare(2);

        final TaxiRide ride3 = testRide(3);
        final TaxiFare fare3 = testFare(3);

        final TaxiRide ride4 = testRide(4);
        final TaxiFare fare4 = testFare(4);

        ParallelTestSource<TaxiRide> rides = new ParallelTestSource<>(ride1, ride4, ride3, ride2);
        ParallelTestSource<TaxiFare> fares = new ParallelTestSource<>(fare2, fare4, fare1, fare3);
        TestSink<RideAndFare> sink = new TestSink<>();

        JobExecutionResult jobResult = ridesAndFaresPipeline().execute(rides, fares, sink);
        assertThat(sink.getResults(jobResult))
                .containsExactlyInAnyOrder(
                        new RideAndFare(ride1, fare1),
                        new RideAndFare(ride2, fare2),
                        new RideAndFare(ride3, fare3),
                        new RideAndFare(ride4, fare4));
    }

    protected ComposedTwoInputPipeline<TaxiRide, TaxiFare, RideAndFare> ridesAndFaresPipeline() {
        ExecutableTwoInputPipeline<TaxiRide, TaxiFare, RideAndFare> exercise =
                (rides, fares, sink) -> (new RidesAndFaresExercise(rides, fares, sink)).execute();

        ExecutableTwoInputPipeline<TaxiRide, TaxiFare, RideAndFare> solution =
                (rides, fares, sink) -> (new RidesAndFaresSolution(rides, fares, sink)).execute();

        return new ComposedTwoInputPipeline<>(exercise, solution);
    }
}
