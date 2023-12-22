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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {
        public static final int ALERT_DURATION_HOURS = 2;
        private transient ValueState<TaxiRide> taxiRideValueState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> taxiRideValueStateDescriptor =
                    new ValueStateDescriptor<>("taxi ride", TaxiRide.class);
            taxiRideValueState = getRuntimeContext().getState(taxiRideValueStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out) throws IOException {
            TaxiRide taxiRideInState = taxiRideValueState.value();
            if (taxiRideInState == null) {
                taxiRideValueState.update(ride);
                if (ride.isStart) {
                    registerTaxiRideDurationTimer(ride, context);
                }
            } else {
                TaxiRide startTaxiRide = ride.isStart ? ride : taxiRideInState;
                TaxiRide endTaxiRide = !ride.isStart ? ride : taxiRideInState;
                if (hasOverpassedAlertDuration(startTaxiRide, endTaxiRide)) {
                    processAlert(context, out, taxiRideInState);
                } else {
                    deleteTaxiRideDurationTimer(context, taxiRideInState);
                }
                taxiRideValueState.clear();
            }
        }

        private void registerTaxiRideDurationTimer(TaxiRide ride, KeyedProcessFunction<Long, TaxiRide, Long>.Context context) {
            context.timerService()
                    .registerEventTimeTimer(ride.eventTime.plus(ALERT_DURATION_HOURS, ChronoUnit.HOURS)
                            .toEpochMilli());
        }

        private void processAlert(KeyedProcessFunction<Long, TaxiRide, Long>.Context context,
                                  Collector<Long> out, TaxiRide taxiRideInState) {
            out.collect(taxiRideInState.rideId);
            taxiRideValueState.clear();
            deleteTaxiRideDurationTimer(context, taxiRideInState);
        }

        private boolean hasOverpassedAlertDuration(TaxiRide startTaxiRide, TaxiRide endTaxiRide) {
            return ChronoUnit.HOURS.between(startTaxiRide.eventTime, endTaxiRide.eventTime) >= ALERT_DURATION_HOURS;
        }

        private void deleteTaxiRideDurationTimer(KeyedProcessFunction<Long, TaxiRide, Long>.Context context,
                                                 TaxiRide taxiRideInState) {
            context.timerService()
                    .deleteEventTimeTimer(taxiRideInState.eventTime.plus(ALERT_DURATION_HOURS, ChronoUnit.HOURS)
                            .toEpochMilli());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws IOException {
            out.collect(taxiRideValueState.value().rideId);
            taxiRideValueState.clear();
        }
    }
}
