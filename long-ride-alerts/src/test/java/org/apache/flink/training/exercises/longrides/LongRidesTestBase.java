package org.apache.flink.training.exercises.longrides;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.time.Instant;

public class LongRidesTestBase {
    public final Instant BEGINNING = Instant.parse("2020-01-01T12:00:00.00Z");
    public final Instant ONE_MINUTE_LATER = BEGINNING.plusSeconds(60);
    public final Instant ONE_HOUR_LATER = BEGINNING.plusSeconds(60 * 60);
    public final Instant TWO_HOURS_LATER = BEGINNING.plusSeconds(120 * 60);
    public final Instant THREE_HOURS_LATER = BEGINNING.plusSeconds(180 * 60);

    public TaxiRide startRide(long rideId, Instant startTime) {
        return testRide(rideId, true, startTime, Instant.EPOCH);
    }

    public TaxiRide endRide(TaxiRide started, Instant endTime) {
        return testRide(started.rideId, false, started.startTime, endTime);
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
}
