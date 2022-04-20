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
