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
