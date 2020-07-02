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

package org.apache.flink.training.exercises.common.utils;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

/**
 * Base for all exercises with a few helper methods.
 */
public class ExerciseBase {
	public static SourceFunction<TaxiRide> rides = null;
	public static SourceFunction<TaxiFare> fares = null;
	public static SourceFunction<String> strings = null;
	public static SinkFunction out = null;
	public static int parallelism = 4;

	/**
	 * Retrieves a test source during unit tests and the given one during normal execution.
	 */
	public static SourceFunction<TaxiRide> rideSourceOrTest(SourceFunction<TaxiRide> source) {
		if (rides == null) {
			return source;
		}
		return rides;
	}

	/**
	 * Retrieves a test source during unit tests and the given one during normal execution.
	 */
	public static SourceFunction<TaxiFare> fareSourceOrTest(SourceFunction<TaxiFare> source) {
		if (fares == null) {
			return source;
		}
		return fares;
	}

	/**
	 * Retrieves a test source during unit tests and the given one during normal execution.
	 */
	public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
		if (strings == null) {
			return source;
		}
		return strings;
	}

	/**
	 * Prints the given data stream during normal execution and collects outputs during tests.
	 */
	public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		} else {
			ds.addSink(out);
		}
	}

	/**
	 * Prints the given data stream during normal execution and collects outputs during tests.
	 */
	public static void printOrTest(org.apache.flink.streaming.api.scala.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		} else {
			ds.addSink(out);
		}
	}
}
