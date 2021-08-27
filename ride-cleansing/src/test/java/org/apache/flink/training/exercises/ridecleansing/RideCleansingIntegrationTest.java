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

package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.ComposedPipeline;
import org.apache.flink.training.exercises.testing.ExecutablePipeline;
import org.apache.flink.training.exercises.testing.ParallelTestSource;
import org.apache.flink.training.exercises.testing.TestSink;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class RideCleansingIntegrationTest extends RideCleansingTestBase {

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
    public void testAMixtureOfLocations() throws Exception {

        TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
        TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
        TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
        TaxiRide atNorthPole = testRide(0, 90, 0, 90);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(toThePole, fromThePole, atPennStation, atNorthPole);
        TestSink<TaxiRide> sink = new TestSink<>();

        JobExecutionResult jobResult = rideCleansingPipeline().execute(source, sink);
        assertThat(sink.getResults(jobResult)).containsExactly(atPennStation);
    }

    protected ComposedPipeline<TaxiRide, TaxiRide> rideCleansingPipeline() {

        ExecutablePipeline<TaxiRide, TaxiRide> exercise =
                (source, sink) -> (new RideCleansingExercise(source, sink)).execute();
        ExecutablePipeline<TaxiRide, TaxiRide> solution =
                (source, sink) -> (new RideCleansingSolution(source, sink)).execute();

        return new ComposedPipeline<>(exercise, solution);
    }
}
