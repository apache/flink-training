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
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * The Stateful Enrichment exercise from the Flink training.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RidesAndFaresExercise {

    private final SourceFunction<TaxiRide> rideSource;
    private final SourceFunction<TaxiFare> fareSource;
    private final SinkFunction<RideAndFare> sink;

    /** Creates a job using the sources and sink provided. */
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
     * @throws Exception which occurs during job execution.
     * @return {JobExecutionResult}
     */
    public JobExecutionResult execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // A stream of taxi ride START events, keyed by rideId.
        DataStream<TaxiRide> rides =
                env.addSource(rideSource).filter(ride -> ride.isStart).keyBy(ride -> ride.rideId);

        // A stream of taxi fare events, also keyed by rideId.
        DataStream<TaxiFare> fares = env.addSource(fareSource).keyBy(fare -> fare.rideId);

        // Create the pipeline.
        rides.connect(fares).flatMap(new EnrichmentFunction()).addSink(sink);

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

        private ValueState<TaxiRide> taxiRideValueState;
        private ValueState<TaxiFare> taxiFareValueState;
        private ValueState<Boolean> processedValueState;

        /***
         *
         * Initializes the two states. One state for the taxiRide and another one for the taxiFare
         *
         * @param config The configuration containing the parameters attached to the contract.
         * @throws Exception
         */
        @Override
        public void open(Configuration config) throws Exception {
            taxiRideValueState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("taxiRide", TaxiRide.class));
            taxiFareValueState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiFare>("taxiFare", TaxiFare.class));
            processedValueState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("processed", Boolean.class));
        }

        /**
         *
         * In case a taxiRide stream element arrives and a taxiFare of the same key (rideId) is stored in the state, then we can add a new {@link RideAndFare} element to the output stream
         * Finally we clear both states (taxiFare and taxiRide) to avoid duplicates
         *
         * @param ride The stream element
         * @param out The collector to emit resulting elements to
         * @throws Exception
         */
        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
            if(Boolean.TRUE.equals(processedValueState.value())) {
                return;
            }
            TaxiFare taxiFare = taxiFareValueState.value();
            if (taxiFare != null) {
                clearState();
                out.collect(new RideAndFare(ride, taxiFare));
                processedValueState.update(true);
            } else {
                taxiRideValueState.update(ride);
            }
        }

        /**
         * In case a taxiFare stream element arrives and a taxiRide of the same key (rideId) is stored in the state, then we can add a new {@link RideAndFare} element to the output stream
         * Finally we clear both states (taxiFare and taxiRide) to avoid duplicates
         * @param fare The stream element
         * @param out The collector to emit resulting elements to
         * @throws Exception
         */
        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
            if(Boolean.TRUE.equals(processedValueState.value())) {
                return;
            }
            TaxiRide taxiRide = taxiRideValueState.value();
            if (taxiRide != null) {
                clearState();
                out.collect(new RideAndFare(taxiRide, fare));
                processedValueState.update(true);
            } else {
                taxiFareValueState.update(fare);
            }
        }

        private void clearState() {
            taxiRideValueState.clear();
            taxiFareValueState.clear();
        }
    }
}
