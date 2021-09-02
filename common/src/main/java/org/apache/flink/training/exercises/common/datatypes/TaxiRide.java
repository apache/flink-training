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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.training.exercises.common.utils.DataGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * <p>A TaxiRide consists of - the rideId of the event which is identical for start and end record -
 * the type of the event (start or end) - the time of the event - the longitude of the start
 * location - the latitude of the start location - the longitude of the end location - the latitude
 * of the end location - the passengerCnt of the ride - the taxiId - the driverId
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    /** Creates a new TaxiRide with now as start and end time. */
    public TaxiRide() {
        this.eventTime = Instant.now();
    }

    /** Invents a TaxiRide. */
    public TaxiRide(long rideId, boolean isStart) {
        DataGenerator g = new DataGenerator(rideId);

        this.rideId = rideId;
        this.isStart = isStart;
        this.eventTime = isStart ? g.startTime() : g.endTime();
        this.startLon = g.startLon();
        this.startLat = g.startLat();
        this.endLon = g.endLon();
        this.endLat = g.endLat();
        this.passengerCnt = g.passengerCnt();
        this.taxiId = g.taxiId();
        this.driverId = g.driverId();
    }

    /** Creates a TaxiRide with the given parameters. */
    public TaxiRide(
            long rideId,
            boolean isStart,
            Instant eventTime,
            float startLon,
            float startLat,
            float endLon,
            float endLat,
            short passengerCnt,
            long taxiId,
            long driverId) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.eventTime = eventTime;
        this.startLon = startLon;
        this.startLat = startLat;
        this.endLon = endLon;
        this.endLat = endLat;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
    }

    public long rideId;
    public boolean isStart;
    public Instant eventTime;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public short passengerCnt;
    public long taxiId;
    public long driverId;

    @Override
    public String toString() {

        return rideId
                + ","
                + (isStart ? "START" : "END")
                + ","
                + eventTime.toString()
                + ","
                + startLon
                + ","
                + startLat
                + ","
                + endLon
                + ","
                + endLat
                + ","
                + passengerCnt
                + ","
                + taxiId
                + ","
                + driverId;
    }

    /**
     * Compares this TaxiRide with the given one.
     *
     * <ul>
     *   <li>sort by timestamp,
     *   <li>putting START events before END events if they have the same timestamp
     * </ul>
     */
    public int compareTo(@Nullable TaxiRide other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = this.eventTime.compareTo(other.eventTime);
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            } else {
                if (this.isStart) {
                    return -1;
                } else {
                    return 1;
                }
            }
        } else {
            return compareTimes;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaxiRide taxiRide = (TaxiRide) o;
        return rideId == taxiRide.rideId
                && isStart == taxiRide.isStart
                && Float.compare(taxiRide.startLon, startLon) == 0
                && Float.compare(taxiRide.startLat, startLat) == 0
                && Float.compare(taxiRide.endLon, endLon) == 0
                && Float.compare(taxiRide.endLat, endLat) == 0
                && passengerCnt == taxiRide.passengerCnt
                && taxiId == taxiRide.taxiId
                && driverId == taxiRide.driverId
                && Objects.equals(eventTime, taxiRide.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rideId,
                isStart,
                eventTime,
                startLon,
                startLat,
                endLon,
                endLat,
                passengerCnt,
                taxiId,
                driverId);
    }

    /** Gets the ride's time stamp as a long in millis since the epoch. */
    public long getEventTimeMillis() {
        return eventTime.toEpochMilli();
    }

    /** Gets the distance from the ride location to the given one. */
    public double getEuclideanDistance(double longitude, double latitude) {
        if (this.isStart) {
            return GeoUtils.getEuclideanDistance(
                    (float) longitude, (float) latitude, this.startLon, this.startLat);
        } else {
            return GeoUtils.getEuclideanDistance(
                    (float) longitude, (float) latitude, this.endLon, this.endLat);
        }
    }

    /** Creates a StreamRecord, using the ride and its timestamp. Used in tests. */
    @VisibleForTesting
    public StreamRecord<TaxiRide> asStreamRecord() {
        return new StreamRecord<>(this, this.getEventTimeMillis());
    }

    /** Creates a StreamRecord from this taxi ride, using its id and timestamp. Used in tests. */
    @VisibleForTesting
    public StreamRecord<Long> idAsStreamRecord() {
        return new StreamRecord<>(this.rideId, this.getEventTimeMillis());
    }
}
