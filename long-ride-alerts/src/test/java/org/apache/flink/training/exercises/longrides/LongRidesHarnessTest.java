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

public class LongRidesHarnessTest extends LongRidesTestBase {

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> harness;

    private KeyedProcessFunction<Long, TaxiRide, Long> javaExercise =
            new LongRidesExercise.AlertFunction();

    private KeyedProcessFunction<Long, TaxiRide, Long> javaSolution =
            new LongRidesSolution.AlertFunction();

    protected ComposedKeyedProcessFunction composedAlertFunction() {
        return new ComposedKeyedProcessFunction(javaExercise, javaSolution);
    }

    @Before
    public void setupTestHarness() throws Exception {
        this.harness = setupHarness(composedAlertFunction());
    }

    @Test
    public void shouldAlertOnLongEndEvent() throws Exception {

        TaxiRide rideStartedButNotSent = startRide(1, BEGINNING);
        TaxiRide endedThreeHoursLater = endRide(rideStartedButNotSent, THREE_HOURS_LATER);

        harness.processElement(endedThreeHoursLater.asStreamRecord());
        assertThat(harness.getOutput()).containsExactly(endedThreeHoursLater.idAsStreamRecord());
    }

    @Test
    public void shouldNotAlertOnShortEndEvent() throws Exception {

        TaxiRide rideStartedButNotSent = startRide(1, BEGINNING);
        TaxiRide endedOneMinuteLater = endRide(rideStartedButNotSent, ONE_MINUTE_LATER);

        harness.processElement(endedOneMinuteLater.asStreamRecord());
        assertThat(harness.getOutput()).isEmpty();
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

        // Check that no state or timers are left behind
        assertThat(harness.numKeyedStateEntries()).isZero();
        assertThat(harness.numEventTimeTimers()).isZero();
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> setupHarness(
            KeyedProcessFunction<Long, TaxiRide, Long> function) throws Exception {

        KeyedProcessOperator<Long, TaxiRide, Long> operator = new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, (TaxiRide r) -> r.rideId, Types.LONG);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
}
