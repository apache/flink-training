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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * A TaxiFare is a taxi fare event.
 *
 * <p>A TaxiFare consists of
 * - the rideId of the event
 * - the time of the event
 */
public class TaxiFare implements Serializable {

	private static final DateTimeFormatter TIME_FORMATTER =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

	/**
	 * Creates a TaxiFare with now as start time.
	 */
	public TaxiFare() {
		this.startTime = new DateTime();
	}

	/**
	 * Creates a TaxiFare with the given parameters.
	 */
	public TaxiFare(long rideId, long taxiId, long driverId, DateTime startTime, String paymentType, float tip, float tolls, float totalFare) {
		this.rideId = rideId;
		this.taxiId = taxiId;
		this.driverId = driverId;
		this.startTime = startTime;
		this.paymentType = paymentType;
		this.tip = tip;
		this.tolls = tolls;
		this.totalFare = totalFare;
	}

	public long rideId;
	public long taxiId;
	public long driverId;
	public DateTime startTime;
	public String paymentType;
	public float tip;
	public float tolls;
	public float totalFare;

	@Override
	public String toString() {
		return rideId + "," +
				taxiId + "," +
				driverId + "," +
				startTime.toString(TIME_FORMATTER) + "," +
				paymentType + "," +
				tip + "," +
				tolls + "," +
				totalFare;
	}

	/**
	 * Parse a TaxiFare from a CSV representation.
	 */
	public static TaxiFare fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 8) {
			throw new RuntimeException("Invalid record: " + line);
		}

		TaxiFare ride = new TaxiFare();

		try {
			ride.rideId = Long.parseLong(tokens[0]);
			ride.taxiId = Long.parseLong(tokens[1]);
			ride.driverId = Long.parseLong(tokens[2]);
			ride.startTime = DateTime.parse(tokens[3], TIME_FORMATTER);
			ride.paymentType = tokens[4];
			ride.tip = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
			ride.tolls = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
			ride.totalFare = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return ride;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiFare &&
				this.rideId == ((TaxiFare) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int) this.rideId;
	}

	/**
	 * Gets the fare's start time.
	 */
	public long getEventTime() {
		return startTime.getMillis();
	}
}
