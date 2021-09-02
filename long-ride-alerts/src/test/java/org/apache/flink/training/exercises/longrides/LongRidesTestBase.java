package org.apache.flink.training.exercises.longrides;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.time.Instant;

public class LongRidesTestBase {
    public static final Instant BEGINNING = Instant.parse("2020-01-01T12:00:00.00Z");
    public static final Instant ONE_MINUTE_LATER = BEGINNING.plusSeconds(60);
    public static final Instant ONE_HOUR_LATER = BEGINNING.plusSeconds(60 * 60);
    public static final Instant TWO_HOURS_LATER = BEGINNING.plusSeconds(120 * 60);
    public static final Instant THREE_HOURS_LATER = BEGINNING.plusSeconds(180 * 60);

    public static TaxiRide startRide(long rideId, Instant startTime) {
        return testRide(rideId, true, startTime);
    }

    public static TaxiRide endRide(TaxiRide started, Instant endTime) {
        return testRide(started.rideId, false, endTime);
    }

    private static TaxiRide testRide(long rideId, Boolean isStart, Instant eventTime) {

        return new TaxiRide(
                rideId,
                isStart,
                eventTime,
                -73.9947F,
                40.750626F,
                -73.9947F,
                40.750626F,
                (short) 1,
                0,
                0);
    }
}
