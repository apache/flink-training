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
import org.apache.flink.training.exercises.testing.TaxiRideTestBase;
import org.apache.flink.training.solutions.longrides.LongRidesSolution;

import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LongRidesTest extends TaxiRideTestBase<TaxiRide> {

	static final Testable JAVA_EXERCISE = () -> LongRidesExercise.main(new String[]{});

	private static final Instant BEGINNING = Instant.parse("2020-01-01T12:00:00.00Z");

	@Test
	public void shortRide() throws Exception {
		Instant oneMinLater = BEGINNING.plusSeconds(60);
		TaxiRide rideStarted = startRide(1, BEGINNING);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.toEpochMilli();

		TestRideSource source = new TestRideSource(rideStarted, endedOneMinLater, markOneMinLater);
		assert(results(source).isEmpty());
	}

	@Test
	public void outOfOrder() throws Exception {
		Instant oneMinLater = BEGINNING.plusSeconds(60);
		TaxiRide rideStarted = startRide(1, BEGINNING);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.toEpochMilli();

		TestRideSource source = new TestRideSource(endedOneMinLater, rideStarted, markOneMinLater);
		assert(results(source).isEmpty());
	}

	@Test
	public void noStartShort() throws Exception {
		Instant oneMinLater = BEGINNING.plusSeconds(60);
		TaxiRide rideStarted = startRide(1, BEGINNING);
		TaxiRide endedOneMinLater = endRide(rideStarted, oneMinLater);
		Long markOneMinLater = oneMinLater.toEpochMilli();

		TestRideSource source = new TestRideSource(endedOneMinLater, markOneMinLater);
		assert(results(source).isEmpty());
	}

	@Test
	public void noEnd() throws Exception {
		TaxiRide rideStarted = startRide(1, BEGINNING);
		Long markThreeHoursLater = BEGINNING.plusSeconds(180 * 60).toEpochMilli();

		TestRideSource source = new TestRideSource(rideStarted, markThreeHoursLater);
		assertEquals(Collections.singletonList(rideStarted), results(source));
	}

	@Test
	public void longRide() throws Exception {
		TaxiRide rideStarted = startRide(1, BEGINNING);
		Long mark2HoursLater = BEGINNING.plusSeconds(120 * 60).toEpochMilli();
		TaxiRide rideEnded3HoursLater = endRide(rideStarted, BEGINNING.plusSeconds(180 * 60));

		TestRideSource source = new TestRideSource(rideStarted, mark2HoursLater, rideEnded3HoursLater);
		assertEquals(Collections.singletonList(rideStarted), results(source));
	}

	@Test
	public void startIsDelayedMoreThanTwoHours() throws Exception {
		TaxiRide rideStarted = startRide(1, BEGINNING);
		TaxiRide rideEndedAfter1Hour = endRide(rideStarted, BEGINNING.plusSeconds(60 * 60));
		Long mark2HoursAfterEnd = BEGINNING.plusSeconds(180 * 60).toEpochMilli();

		TestRideSource source = new TestRideSource(rideEndedAfter1Hour, mark2HoursAfterEnd, rideStarted);
		assert(results(source).isEmpty());
	}

	private TaxiRide testRide(long rideId, Boolean isStart, Instant startTime, Instant endTime) {
		return new TaxiRide(rideId, isStart, startTime, endTime, -73.9947F, 40.750626F, -73.9947F, 40.750626F, (short) 1, 0, 0);
	}

	private TaxiRide startRide(long rideId, Instant startTime) {
		return testRide(rideId, true, startTime, Instant.EPOCH);
	}

	private TaxiRide endRide(TaxiRide started, Instant endTime) {
		return testRide(started.rideId, false, started.startTime, endTime);
	}

	protected List<TaxiRide> results(TestRideSource source) throws Exception {
		Testable javaSolution = () -> LongRidesSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), JAVA_EXERCISE, javaSolution);
	}

}
