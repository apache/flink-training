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

import org.apache.flink.training.exercises.common.utils.GeoUtils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Locale;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * <p>A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

	private static final DateTimeFormatter TIME_FORMATTER =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

	/**
	 * Creates a new TaxiRide with now as start and end time.
	 */
	public TaxiRide() {
		this.startTime = new DateTime();
		this.endTime = new DateTime();
	}

	/**
	 * Creates a TaxiRide with the given parameters.
	 */
	public TaxiRide(long rideId, boolean isStart, DateTime startTime, DateTime endTime,
					float startLon, float startLat, float endLon, float endLat,
					short passengerCnt, long taxiId, long driverId) {
		this.rideId = rideId;
		this.isStart = isStart;
		this.startTime = startTime;
		this.endTime = endTime;
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
	public DateTime startTime;
	public DateTime endTime;
	public float startLon;
	public float startLat;
	public float endLon;
	public float endLat;
	public short passengerCnt;
	public long taxiId;
	public long driverId;

	@Override
	public String toString() {
		return rideId + "," +
				(isStart ? "START" : "END") + "," +
				startTime.toString(TIME_FORMATTER) + "," +
				endTime.toString(TIME_FORMATTER) + "," +
				startLon + "," +
				startLat + "," +
				endLon + "," +
				endLat + "," +
				passengerCnt + "," +
				taxiId + "," +
				driverId;
	}

	/**
	 * Parse a TaxiRide from a CSV representation.
	 */
	public static TaxiRide fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 11) {
			throw new RuntimeException("Invalid record: " + line);
		}

		TaxiRide ride = new TaxiRide();

		try {
			ride.rideId = Long.parseLong(tokens[0]);

			switch (tokens[1]) {
				case "START":
					ride.isStart = true;
					ride.startTime = DateTime.parse(tokens[2], TIME_FORMATTER);
					ride.endTime = DateTime.parse(tokens[3], TIME_FORMATTER);
					break;
				case "END":
					ride.isStart = false;
					ride.endTime = DateTime.parse(tokens[2], TIME_FORMATTER);
					ride.startTime = DateTime.parse(tokens[3], TIME_FORMATTER);
					break;
				default:
					throw new RuntimeException("Invalid record: " + line);
			}

			ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
			ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
			ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
			ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
			ride.passengerCnt = Short.parseShort(tokens[8]);
			ride.taxiId = Long.parseLong(tokens[9]);
			ride.driverId = Long.parseLong(tokens[10]);

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return ride;
	}

	/**
	 * Compares this TaxiRide with the given one.
	 *
	 * <ul>
	 *     <li>sort by timestamp,</li>
	 *     <li>putting START events before END events if they have the same timestamp</li>
	 * </ul>
	 */
	public int compareTo(@Nullable TaxiRide other) {
		if (other == null) {
			return 1;
		}
		int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
		if (compareTimes == 0) {
			if (this.isStart == other.isStart) {
				return 0;
			}
			else {
				if (this.isStart) {
					return -1;
				}
				else {
					return 1;
				}
			}
		}
		else {
			return compareTimes;
		}
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiRide &&
				this.rideId == ((TaxiRide) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int) this.rideId;
	}

	/**
	 * Gets the ride's time stamp (start or end time depending on {@link #isStart}).
	 */
	public long getEventTime() {
		if (isStart) {
			return startTime.getMillis();
		}
		else {
			return endTime.getMillis();
		}
	}

	/**
	 * Gets the distance from the ride location to the given one.
	 */
	public double getEuclideanDistance(double longitude, double latitude) {
		if (this.isStart) {
			return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat);
		} else {
			return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
		}
	}
}
