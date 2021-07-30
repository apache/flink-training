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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.ComposedPipeline;
import org.apache.flink.training.exercises.testing.ExecutablePipeline;
import org.apache.flink.training.exercises.testing.ParallelTestSource;
import org.apache.flink.training.exercises.testing.TestSink;
import org.apache.flink.training.exercises.testing.TestSourcePartitioner;
import org.apache.flink.training.solutions.longrides.LongRidesSolution;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LongRidesTest {

    private static final int PARALLELISM = 2;

    /** This isn't necessary, but speeds up the tests. */
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setNumberTaskManagers(1)
                            .build());

    private static final Instant BEGINNING = Instant.parse("2020-01-01T12:00:00.00Z");
    private static final Instant ONE_MINUTE_LATER = BEGINNING.plusSeconds(60);
    private static final Instant THREE_HOURS_LATER = BEGINNING.plusSeconds(180 * 60);

    @Test
    public void shortRide() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinLater = endRide(rideStarted, ONE_MINUTE_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(new PartitionByRideId(), rideStarted, endedOneMinLater);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results()).isEmpty();
    }

    @Test
    public void outOfOrder() throws Exception {
        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinLater = endRide(rideStarted, ONE_MINUTE_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(new PartitionByRideId(), endedOneMinLater, rideStarted);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results()).isEmpty();
    }

    @Test
    public void noStartShort() throws Exception {
        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinLater = endRide(rideStarted, ONE_MINUTE_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(new PartitionByRideId(), endedOneMinLater);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results()).isEmpty();
    }

    @Test
    public void noStartLong() throws Exception {
        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedThreeHoursLater = endRide(rideStarted, THREE_HOURS_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(new PartitionByRideId(), endedThreeHoursLater);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results()).containsExactly(rideStarted.rideId);
    }

    @Test
    public void endIsMissing() throws Exception {
        TaxiRide rideStarted = startRide(1, BEGINNING);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(new PartitionByRideId(), rideStarted);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results()).containsExactly(rideStarted.rideId);
    }

    @Test
    public void endComesAfter3Hours() throws Exception {
        TaxiRide startOfLongRide = startRide(1, BEGINNING);
        TaxiRide longRideEndedAfter3Hours = endRide(startOfLongRide, THREE_HOURS_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(
                        new PartitionByRideId(), startOfLongRide, longRideEndedAfter3Hours);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results()).containsExactly(startOfLongRide.rideId);
    }

    @Test
    public void multipleRides() throws Exception {
        TaxiRide startOfOneRide = startRide(1, BEGINNING);
        TaxiRide otherRide = startRide(2, ONE_MINUTE_LATER);
        TaxiRide oneRideEnded = endRide(startOfOneRide, THREE_HOURS_LATER);
        TaxiRide otherRideEnded = endRide(otherRide, THREE_HOURS_LATER);

        ParallelTestSource<TaxiRide> source =
                new ParallelTestSource<>(
                        new PartitionByRideId(),
                        startOfOneRide,
                        otherRide,
                        oneRideEnded,
                        otherRideEnded);
        TestSink<Long> sink = new TestSink<Long>();

        longRidesPipeline().execute(source, sink);
        assertThat(sink.results())
                .containsExactlyInAnyOrder(startOfOneRide.rideId, otherRide.rideId);
    }

    // Arranges for all events for a given rideId to be generated by the same source subtask.
    private static class PartitionByRideId implements TestSourcePartitioner<TaxiRide> {
        @Override
        public long partition(TaxiRide ride) {
            return ride.rideId;
        }
    }

    private TaxiRide testRide(long rideId, Boolean isStart, Instant startTime, Instant endTime) {
        return new TaxiRide(
                rideId,
                isStart,
                startTime,
                endTime,
                -73.9947F,
                40.750626F,
                -73.9947F,
                40.750626F,
                (short) 1,
                0,
                0);
    }

    private TaxiRide startRide(long rideId, Instant startTime) {
        return testRide(rideId, true, startTime, Instant.EPOCH);
    }

    private TaxiRide endRide(TaxiRide started, Instant endTime) {
        return testRide(started.rideId, false, started.startTime, endTime);
    }

    protected ComposedPipeline longRidesPipeline() {
        ExecutablePipeline<TaxiRide, Long> exercise =
                (source, sink) -> (new LongRidesExercise(source, sink)).execute();
        ExecutablePipeline<TaxiRide, Long> solution =
                (source, sink) -> (new LongRidesSolution(source, sink)).execute();

        return new ComposedPipeline(exercise, solution);
    }
}
