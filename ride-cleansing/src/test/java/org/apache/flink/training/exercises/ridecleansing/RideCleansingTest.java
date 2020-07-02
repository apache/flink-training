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

package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.testing.TaxiRideTestBase;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;

import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RideCleansingTest extends TaxiRideTestBase<TaxiRide> {

	static final Testable JAVA_EXERCISE = () -> RideCleansingExercise.main(new String[]{});

	@Test
	public void testInNYC() throws Exception {
		TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);

		TestRideSource source = new TestRideSource(atPennStation);

		assertEquals(Collections.singletonList(atPennStation), results(source));
	}

	@Test
	public void testNotInNYC() throws Exception {
		TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
		TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
		TaxiRide atNorthPole = testRide(0, 90, 0, 90);

		TestRideSource source = new TestRideSource(toThePole, fromThePole, atNorthPole);

		assertEquals(Collections.emptyList(), results(source));
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, Instant.EPOCH, Instant.EPOCH,
				startLon, startLat, endLon, endLat, (short) 1, 0, 0);
	}

	protected List<?> results(TestRideSource source) throws Exception {
		Testable javaSolution = () -> RideCleansingSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), JAVA_EXERCISE, javaSolution);
	}

}
