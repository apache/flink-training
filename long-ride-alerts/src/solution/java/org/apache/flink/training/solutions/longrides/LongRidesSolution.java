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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import java.time.Duration;

/**
 * Java solution for the "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesSolution {

    private SourceFunction<TaxiRide> source;
    private SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesSolution(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * <p>@throws Exception which occurs during job execution.
     */
    public void execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source, TypeInformation.of(TaxiRide.class));

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTime());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline
        env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesSolution job =
                new LongRidesSolution(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> stateDescriptor =
                    new ValueStateDescriptor<>("ride event", TaxiRide.class);
            rideState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {

            TaxiRide firstRideEvent = rideState.value();

            if (firstRideEvent == null) {
                rideState.update(ride);

                if (ride.isStart) {
                    context.timerService().registerEventTimeTimer(getTimerTime(ride));
                } else {
                    if (rideTooLong(ride)) {
                        out.collect(ride.rideId);
                    }
                }
            } else {
                if (ride.isStart) {
                    // There's nothing to do but clear the state (which is done below).
                } else {
                    // There may be a timer that hasn't fired yet.
                    context.timerService().deleteEventTimeTimer(getTimerTime(firstRideEvent));

                    // It could be that the ride has gone on too long, but the timer hasn't fired
                    // yet.
                    if (rideTooLong(ride)) {
                        out.collect(ride.rideId);
                    }
                }
                // Both events have now been seen, we can clear the state.
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {

            // The timer only fires if the ride was too long.
            out.collect(rideState.value().rideId);
            rideState.clear();
        }

        private boolean rideTooLong(TaxiRide rideEndEvent) {
            return Duration.between(rideEndEvent.startTime, rideEndEvent.endTime)
                            .compareTo(Duration.ofHours(2))
                    > 0;
        }

        private long getTimerTime(TaxiRide ride) {
            return ride.startTime.plusSeconds(120 * 60).toEpochMilli();
        }
    }
}
