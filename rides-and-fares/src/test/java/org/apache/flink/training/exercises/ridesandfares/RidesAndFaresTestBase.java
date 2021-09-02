package org.apache.flink.training.exercises.ridesandfares;

import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.time.Instant;

public class RidesAndFaresTestBase {

    public static TaxiRide testRide(long rideId) {
        return new TaxiRide(rideId, true, Instant.EPOCH, 0F, 0F, 0F, 0F, (short) 1, 0, rideId);
    }

    public static TaxiFare testFare(long rideId) {
        return new TaxiFare(rideId, 0, rideId, Instant.EPOCH, "", 0F, 0F, 0F);
    }
}
