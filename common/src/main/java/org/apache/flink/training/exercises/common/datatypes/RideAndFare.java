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
