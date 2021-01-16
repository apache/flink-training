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

package org.apache.flink.training.solutions.longrides;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

/**
 * Solution to the "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */
public class LongRidesSolution extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

		DataStream<TaxiRide> longRides = rides
				.keyBy((TaxiRide ride) -> ride.rideId)
				.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	private static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		private ValueState<TaxiRide> rideState;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<TaxiRide> stateDescriptor =
					new ValueStateDescriptor<>("ride event", TaxiRide.class);
			rideState = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TaxiRide previousRideEvent = rideState.value();

			if (previousRideEvent == null) {
				rideState.update(ride);
				if (ride.isStart) {
					context.timerService().registerEventTimeTimer(getTimerTime(ride));
				}
			} else {
				if (!ride.isStart) {
					// it's an END event, so event saved was the START event and has a timer
					// the timer hasn't fired yet, and we can safely kill the timer
					context.timerService().deleteEventTimeTimer(getTimerTime(previousRideEvent));
				}
				// both events have now been seen, we can clear the state
				rideState.clear();
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {

			// if we get here, we know that the ride started two hours ago, and the END hasn't been processed
			out.collect(rideState.value());
			rideState.clear();
		}

		private long getTimerTime(TaxiRide ride) {
			return ride.startTime.plusSeconds(120 * 60).toEpochMilli();
		}
	}

}
