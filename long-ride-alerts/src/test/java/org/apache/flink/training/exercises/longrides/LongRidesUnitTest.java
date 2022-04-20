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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.ComposedKeyedProcessFunction;
import org.apache.flink.training.solutions.longrides.LongRidesSolution;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

// needed for the Scala tests to use scala.Long with this Java test
@SuppressWarnings({"rawtypes", "unchecked"})
public class LongRidesUnitTest extends LongRidesTestBase {

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> harness;

    private final KeyedProcessFunction<Long, TaxiRide, Long> javaExercise =
            new LongRidesExercise.AlertFunction();

    private final KeyedProcessFunction<Long, TaxiRide, Long> javaSolution =
            new LongRidesSolution.AlertFunction();

    protected ComposedKeyedProcessFunction composedAlertFunction() {
        return new ComposedKeyedProcessFunction<>(javaExercise, javaSolution);
    }

    @Before
    public void setupTestHarness() throws Exception {
        this.harness = setupHarness(composedAlertFunction());
    }

    @Test
    public void shouldUseTimersAndState() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        harness.processElement(rideStarted.asStreamRecord());

        // check for state and timers
        assertThat(harness.numEventTimeTimers()).isGreaterThan(0);
        assertThat(harness.numKeyedStateEntries()).isGreaterThan(0);

        TaxiRide endedOneMinuteLater = endRide(rideStarted, ONE_MINUTE_LATER);
        harness.processElement(endedOneMinuteLater.asStreamRecord());

        // in this case, state and timers should be gone now
        assertThat(harness.numEventTimeTimers()).isZero();
        assertThat(harness.numKeyedStateEntries()).isZero();
    }

    @Test
    public void shouldNotAlertWithStartFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinuteLater = endRide(rideStarted, ONE_MINUTE_LATER);

        harness.processElement(rideStarted.asStreamRecord());
        harness.processElement(endedOneMinuteLater.asStreamRecord());

        assertThat(harness.getOutput()).isEmpty();
    }

    @Test
    public void shouldNotAlertWithEndFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinuteLater = endRide(rideStarted, ONE_MINUTE_LATER);

        harness.processElement(endedOneMinuteLater.asStreamRecord());
        harness.processElement(rideStarted.asStreamRecord());

        assertThat(harness.getOutput()).isEmpty();
    }

    @Test
    public void shouldAlertWithStartFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedThreeHoursLater = endRide(rideStarted, THREE_HOURS_LATER);

        harness.processElement(rideStarted.asStreamRecord());
        harness.processElement(endedThreeHoursLater.asStreamRecord());

        assertThat(resultingRideId()).isEqualTo(rideStarted.rideId);
    }

    @Test
    public void shouldAlertWithEndFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedThreeHoursLater = endRide(rideStarted, THREE_HOURS_LATER);

        harness.processElement(endedThreeHoursLater.asStreamRecord());
        harness.processElement(rideStarted.asStreamRecord());

        assertThat(resultingRideId()).isEqualTo(rideStarted.rideId);
    }

    @Test
    public void shouldNotAlertWithoutWatermarkOrEndEvent() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide otherRideStartsThreeHoursLater = startRide(2, THREE_HOURS_LATER);

        harness.processElement(rideStarted.asStreamRecord());
        harness.processElement(otherRideStartsThreeHoursLater.asStreamRecord());
        assertThat(harness.getOutput()).isEmpty();

        Watermark mark2HoursLater =
                new Watermark(BEGINNING.plusSeconds(2 * 60 * 60).toEpochMilli());
        harness.processWatermark(mark2HoursLater);

        StreamRecord<Long> rideIdAtTimeOfWatermark =
                new StreamRecord<>(rideStarted.rideId, mark2HoursLater.getTimestamp());
        assertThat(harness.getOutput()).containsExactly(rideIdAtTimeOfWatermark, mark2HoursLater);
    }

    @Test
    public void shouldAlertOnWatermark() throws Exception {

        TaxiRide startOfLongRide = startRide(1, BEGINNING);
        harness.processElement(startOfLongRide.asStreamRecord());

        // Can't be done properly without some managed keyed state
        assertThat(harness.numKeyedStateEntries()).isGreaterThan(0);

        // At this point there should be no output
        ConcurrentLinkedQueue<Object> initialOutput = harness.getOutput();
        assertThat(initialOutput).isEmpty();

        Watermark mark2HoursLater =
                new Watermark(BEGINNING.plusSeconds(2 * 60 * 60).toEpochMilli());
        harness.processWatermark(mark2HoursLater);

        // Check that the result is correct
        StreamRecord<Long> rideIdAtTimeOfWatermark =
                new StreamRecord<>(startOfLongRide.rideId, mark2HoursLater.getTimestamp());
        assertThat(harness.getOutput()).containsExactly(rideIdAtTimeOfWatermark, mark2HoursLater);
    }

    private Long resultingRideId() {
        ConcurrentLinkedQueue<Object> results = harness.getOutput();
        assertThat(results.size())
                .withFailMessage("Expecting test to have exactly one result")
                .isEqualTo(1);
        StreamRecord<Long> resultingRecord = (StreamRecord<Long>) results.element();
        return resultingRecord.getValue();
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> setupHarness(
            KeyedProcessFunction<Long, TaxiRide, Long> function) throws Exception {

        KeyedProcessOperator<Long, TaxiRide, Long> operator = new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, r -> r.rideId, Types.LONG);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
}
