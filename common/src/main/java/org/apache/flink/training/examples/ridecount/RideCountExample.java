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

package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;

/**
 * Example that counts the rides for each driver.
 *
 * <p>Note that this is implicitly keeping state for each driver. This sort of simple, non-windowed
 * aggregation on an unbounded set of keys will use an unbounded amount of state. When this is an
 * issue, look at the SQL/Table API, or ProcessFunction, or state TTL, all of which provide
 * mechanisms for expiring state for stale keys.
 */
public class RideCountExample {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        // map each ride to a tuple of (driverId, 1)
        DataStream<Tuple2<Long, Long>> tuples =
                rides.map(
                        new MapFunction<TaxiRide, Tuple2<Long, Long>>() {
                            @Override
                            public Tuple2<Long, Long> map(TaxiRide ride) {
                                return Tuple2.of(ride.driverId, 1L);
                            }
                        });

        // partition the stream by the driverId
        KeyedStream<Tuple2<Long, Long>, Long> keyedByDriverId = tuples.keyBy(t -> t.f0);

        // count the rides for each driver
        DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId.sum(1);

        // we could, in fact, print out any or all of these streams
        rideCounts.print();

        // run the cleansing pipeline
        env.execute("Ride Count");
    }
}
