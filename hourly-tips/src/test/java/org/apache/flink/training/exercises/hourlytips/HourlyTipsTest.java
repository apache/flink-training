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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.testing.TaxiRideTestBase;
import org.apache.flink.training.solutions.hourlytips.HourlyTipsSolution;

import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HourlyTipsTest extends TaxiRideTestBase<Tuple3<Long, Long, Float>> {

	static final Testable JAVA_EXERCISE = () -> HourlyTipsExercise.main(new String[]{});

	@Test
	public void testOneDriverOneTip() throws Exception {
		TaxiFare one = testFare(1, t(0), 1.0F);

		TestFareSource source = new TestFareSource(
				one
		);

		Tuple3<Long, Long, Float> max = Tuple3.of(t(60).toEpochMilli(), 1L, 1.0F);

		List<Tuple3<Long, Long, Float>> expected = Collections.singletonList(max);

		assertEquals(expected, results(source));
	}

	@Test
	public void testTipsAreSummedByHour() throws Exception {
		TaxiFare oneIn1 = testFare(1, t(0), 1.0F);
		TaxiFare fiveIn1 = testFare(1, t(15), 5.0F);
		TaxiFare tenIn2 = testFare(1, t(90), 10.0F);

		TestFareSource source = new TestFareSource(
				oneIn1,
				fiveIn1,
				tenIn2
		);

		Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 1L, 6.0F);
		Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 1L, 10.0F);

		List<Tuple3<Long, Long, Float>> expected = Arrays.asList(hour1, hour2);

		assertEquals(expected, results(source));
	}

	@Test
	public void testMaxAcrossDrivers() throws Exception {
		TaxiFare oneFor1In1 = testFare(1, t(0), 1.0F);
		TaxiFare fiveFor1In1 = testFare(1, t(15), 5.0F);
		TaxiFare tenFor1In2 = testFare(1, t(90), 10.0F);
		TaxiFare twentyFor2In2 = testFare(2, t(90), 20.0F);

		TestFareSource source = new TestFareSource(
				oneFor1In1,
				fiveFor1In1,
				tenFor1In2,
				twentyFor2In2
		);

		Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 1L, 6.0F);
		Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 2L, 20.0F);

		List<Tuple3<Long, Long, Float>> expected = Arrays.asList(hour1, hour2);

		assertEquals(expected, results(source));
	}

	private Instant t(int minutes) {
		return Instant.parse("2020-01-01T12:00:00.00Z").plusSeconds(60 * minutes);
	}

	private TaxiFare testFare(long driverId, Instant startTime, float tip) {
		return new TaxiFare(0, 0, driverId, startTime, "", tip, 0F, 0F);
	}

	protected List<Tuple3<Long, Long, Float>> results(TestFareSource source) throws Exception {
		Testable javaSolution = () -> HourlyTipsSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), JAVA_EXERCISE, javaSolution);
	}

}
