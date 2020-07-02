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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * The "Stateful Enrichment" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 *
 */
public class RidesAndFaresExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideGenerator()))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy((TaxiRide ride) -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareGenerator()))
				.keyBy((TaxiFare fare) -> fare.rideId);

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		printOrTest(enrichedRides);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		@Override
		public void open(Configuration config) throws Exception {
			throw new MissingSolutionException();
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
		}
	}
}
