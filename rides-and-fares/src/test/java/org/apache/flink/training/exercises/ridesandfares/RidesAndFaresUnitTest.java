package org.apache.flink.training.exercises.ridesandfares;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.training.exercises.common.datatypes.RideAndFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.ComposedRichCoFlatMapFunction;
import org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class RidesAndFaresUnitTest extends RidesAndFaresTestBase {

    private KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, RideAndFare> harness;

    private final RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> javaExercise =
            new RidesAndFaresExercise.EnrichmentFunction();

    private final RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> javaSolution =
            new RidesAndFaresSolution.EnrichmentFunction();

    protected ComposedRichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare>
            composedEnrichmentFunction() {
        return new ComposedRichCoFlatMapFunction<>(javaExercise, javaSolution);
    }

    private static final TaxiRide ride1 = testRide(1);
    private static final TaxiFare fare1 = testFare(1);

    @Before
    public void setupTestHarness() throws Exception {
        this.harness = setupHarness(composedEnrichmentFunction());
    }

    @Test
    public void testRideStateCreatedAndCleared() throws Exception {

        // Stream in a ride and check that state was created
        harness.processElement1(ride1.asStreamRecord());
        assertThat(harness.numKeyedStateEntries()).isGreaterThan(0);

        // After processing the matching fare, the state should be cleared
        harness.processElement2(fare1.asStreamRecord());
        assertThat(harness.numKeyedStateEntries()).isZero();

        // Verify the result
        StreamRecord<RideAndFare> expected =
                new StreamRecord<>(new RideAndFare(ride1, fare1), ride1.getEventTime());
        assertThat(harness.getOutput()).containsExactly(expected);
    }

    @Test
    public void testFareStateCreatedAndCleared() throws Exception {

        // Stream in a fare and check that state was created
        harness.processElement2(fare1.asStreamRecord());
        assertThat(harness.numKeyedStateEntries()).isGreaterThan(0);

        // After processing the matching ride, the state should be cleared
        harness.processElement1(ride1.asStreamRecord());
        assertThat(harness.numKeyedStateEntries()).isZero();

        // Verify the result
        StreamRecord<RideAndFare> expected =
                new StreamRecord<>(new RideAndFare(ride1, fare1), ride1.getEventTime());
        assertThat(harness.getOutput()).containsExactly(expected);
    }

    private KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, RideAndFare>
            setupHarness(RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> function)
                    throws Exception {

        TwoInputStreamOperator<TaxiRide, TaxiFare, RideAndFare> operator =
                new CoStreamFlatMap<>(function);

        KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, RideAndFare> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator, r -> r.rideId, f -> f.rideId, Types.LONG);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
}
