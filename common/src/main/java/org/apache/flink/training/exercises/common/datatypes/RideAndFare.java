package org.apache.flink.training.exercises.common.datatypes;

import java.io.Serializable;

/** Holds a TaxiRide and a TaxiFare. */
public class RideAndFare implements Serializable {

    public TaxiRide ride;
    public TaxiFare fare;

    /** Default constructor. */
    public RideAndFare() {}

    /** Create a RideAndFare from the ride and fare provided. */
    public RideAndFare(TaxiRide ride, TaxiFare fare) {
        this.ride = ride;
        this.fare = fare;
    }

    @Override
    public String toString() {
        return "<" + ride.toString() + " / " + fare.toString() + ">";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RideAndFare)) {
            return false;
        }

        RideAndFare otherRandF = (RideAndFare) other;
        return this.ride.equals(otherRandF.ride) && this.fare.equals(otherRandF.fare);
    }
}
