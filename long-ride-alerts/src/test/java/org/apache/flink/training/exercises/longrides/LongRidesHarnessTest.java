package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.solutions.longrides.LongRidesSolution;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LongRidesHarnessTest {

    @Test
    public void testLongRideAlertsAsSoonAsPossible() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> harness = setupHarness();

        TaxiRide startOfLongRide = LongRidesTest.startRide(1, LongRidesTest.BEGINNING);
        harness.processElement(new StreamRecord<>(startOfLongRide, startOfLongRide.getEventTime()));

        Watermark mark2HoursLater =
                new Watermark(LongRidesTest.BEGINNING.plusSeconds(120 * 60).toEpochMilli());
        harness.processWatermark(mark2HoursLater);

        // Check that the result is correct
        ConcurrentLinkedQueue<Object> actualOutput = harness.getOutput();
        StreamRecord<Long> rideIdAtTimeOfWatermark =
                new StreamRecord<>(startOfLongRide.rideId, mark2HoursLater.getTimestamp());
        assertThat(actualOutput).containsExactly(rideIdAtTimeOfWatermark, mark2HoursLater);

        // Check that no state or timers are left behind
        assertThat(harness.numKeyedStateEntries()).isZero();
        assertThat(harness.numEventTimeTimers()).isZero();
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> setupHarness()
            throws Exception {

        KeyedProcessOperator<Long, TaxiRide, Long> operator =
                new KeyedProcessOperator<>(new LongRidesSolution.MatchFunction());

        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, (TaxiRide r) -> r.rideId, BasicTypeInfo.LONG_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
}
