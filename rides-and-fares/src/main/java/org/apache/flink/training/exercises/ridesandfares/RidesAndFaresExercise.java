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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.RideAndFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * The Stateful Enrichment exercise from the Flink training.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RidesAndFaresExercise {

    private final SourceFunction<TaxiRide> rideSource;
    private final SourceFunction<TaxiFare> fareSource;
    private final SinkFunction<RideAndFare> sink;

    /**
     * Creates a job using the sources and sink provided.
     */
    public RidesAndFaresExercise(
            SourceFunction<TaxiRide> rideSource,
            SourceFunction<TaxiFare> fareSource,
            SinkFunction<RideAndFare> sink) {

        this.rideSource = rideSource;
        this.fareSource = fareSource;
        this.sink = sink;
    }

    /**
     * Creates and executes the pipeline using the StreamExecutionEnvironment provided.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // A stream of taxi ride START events, keyed by rideId.
        DataStream<TaxiRide> rides =
                env.addSource(rideSource)
                        .filter(ride -> ride.isStart)
                        .keyBy(ride -> ride.rideId);

        // A stream of taxi fare events, also keyed by rideId.
        DataStream<TaxiFare> fares = env.addSource(fareSource)
                .keyBy(fare -> fare.rideId);

        // Create the pipeline.
        rides.connect(fares)
                .flatMap(new EnrichmentFunction())
                .addSink(sink);

        // Execute the pipeline and return the result.
        return env.execute("Join Rides with Fares");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        RidesAndFaresExercise job =
                new RidesAndFaresExercise(
                        new TaxiRideGenerator(),
                        new TaxiFareGenerator(),
                        new PrintSinkFunction<>());

        job.execute();
    }

    public static class EnrichmentFunction
            extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {

        private ValueState<RideAndFare> rideAndFareValueState;

        @Override
        public void open(Configuration config) throws Exception {
            rideAndFareValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("perRideIdDataHolder", RideAndFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
            if (ride.isStart) {
                RideAndFare rideAndFare = rideAndFareValueState.value();
                if (rideAndFare != null) {
                    rideAndFare.ride = ride;
                    collectJoined(out, rideAndFare);
                } else {
                    rideAndFareValueState.update(new RideAndFare(ride, null));
                }
            } // omit END event
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
            RideAndFare rideAndFare = rideAndFareValueState.value();
            if (rideAndFare != null) {
                rideAndFare.fare = fare;
                collectJoined(out, rideAndFare);
            } else  {
                rideAndFareValueState.update(new RideAndFare(null, fare));
            }
        }

        private void collectJoined(Collector<RideAndFare> out, RideAndFare rideAndFare) {
            out.collect(rideAndFare);
            rideAndFareValueState.clear();
        }
    }
}
