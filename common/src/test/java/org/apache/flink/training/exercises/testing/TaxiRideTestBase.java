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

package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

import java.util.ArrayList;
import java.util.List;

public abstract class TaxiRideTestBase<OUT> {
	public static class TestRideSource extends TestSource<TaxiRide> implements ResultTypeQueryable<TaxiRide> {
		public TestRideSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		long getTimestamp(TaxiRide ride) {
			return ride.getEventTime();
		}

		@Override
		public TypeInformation<TaxiRide> getProducedType() {
			return TypeInformation.of(TaxiRide.class);
		}
	}

	public static class TestFareSource extends TestSource<TaxiFare> implements ResultTypeQueryable<TaxiFare> {
		public TestFareSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		long getTimestamp(TaxiFare fare) {
			return fare.getEventTime();
		}

		@Override
		public TypeInformation<TaxiFare> getProducedType() {
			return TypeInformation.of(TaxiFare.class);
		}
	}

	public static class TestStringSource extends TestSource<String> implements ResultTypeQueryable<String> {
		public TestStringSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		long getTimestamp(String s) {
			return 0L;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return TypeInformation.of(String.class);
		}
	}

	public static class TestSink<OUT> implements SinkFunction<OUT> {

		// must be static
		public static final List VALUES = new ArrayList<>();

		@Override
		public void invoke(OUT value, Context context) {
			VALUES.add(value);
		}
	}

	public interface Testable {
		void main() throws Exception;
	}

	protected List<OUT> runApp(TestRideSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.rides = source;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestFareSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.fares = source;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestRideSource rides, TestFareSource fares, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.rides = rides;
		ExerciseBase.fares = fares;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestRideSource rides, TestSink<OUT> sink, Testable solution) throws Exception {
		ExerciseBase.rides = rides;

		return execute(sink, solution);
	}

	protected List<OUT> runApp(TestRideSource rides, TestStringSource strings, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		ExerciseBase.rides = rides;
		ExerciseBase.strings = strings;

		return execute(sink, exercise, solution);
	}

	private List<OUT> execute(TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		sink.VALUES.clear();

		ExerciseBase.out = sink;
		ExerciseBase.parallelism = 1;

		try {
			exercise.main();
		} catch (Exception e) {
			if (ultimateCauseIsMissingSolution(e)) {
				sink.VALUES.clear();
				solution.main();
			} else {
				throw e;
			}
		}

		return sink.VALUES;
	}

	private List<OUT> execute(TestSink<OUT> sink, Testable solution) throws Exception {
		sink.VALUES.clear();

		ExerciseBase.out = sink;
		ExerciseBase.parallelism = 1;

		solution.main();

		return sink.VALUES;
	}

	private boolean ultimateCauseIsMissingSolution(Throwable e) {
		if (e instanceof MissingSolutionException) {
			return true;
		} else if (e.getCause() != null) {
			return ultimateCauseIsMissingSolution(e.getCause());
		} else {
			return false;
		}
	}
}
