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

import java.time.Duration;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final Duration maxWaitForLateNotifications = Duration.ofSeconds(60);
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

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

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy
                = WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(maxWaitForLateNotifications)
                                 .withTimestampAssigner((ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source).assignTimestampsAndWatermarks(watermarkStrategy);

        // the pipeline
        rides   .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        return env.execute("Long Taxi Rides"); // execute the pipeline and return the result
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private final Duration maxDuration = Duration.ofHours(2);
        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<TaxiRide> rideStateDescriptor =
                    new ValueStateDescriptor<>("ride event", TaxiRide.class);
            this.rideState = getRuntimeContext().getState(rideStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {
            TaxiRide firstRideEvent = rideState.value();

            if (firstRideEvent == null) { // first notification about this ride
                rideState.update(ride);// whatever event comes first, remember it

                if (ride.isStart)  // set timer for ride that has no end (in due time or at all)
                    context.timerService().registerEventTimeTimer(getMaxEndTime(ride));
                return;
            }

            // we now have both ends of this ride

            if (ride.isStart) { // notification of the start after notification of end
                if (rideTooLong(ride, firstRideEvent))  // just check if it was too long
                    out.collect(ride.rideId);
                return;
            }

            // the first ride was a START event, so there is a timer unless it has fired
            context.timerService().deleteEventTimeTimer(getMaxEndTime(firstRideEvent));

            if (rideTooLong(firstRideEvent, ride)) // just in case the timer didn't fire yet
                out.collect(ride.rideId);

            // both events have now been seen, we can clear the state
            // this solution can leak state if an event is missing
            // see DISCUSSION.md for more information
            rideState.clear();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {

            out.collect(rideState.value().rideId);// the timer only fires if the ride was too long

            rideState.clear();// clearing now prevents duplicate alerts, but will leak state if the END arrives
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.eventTime, endEvent.eventTime)
                    .compareTo(maxDuration)
                    > 0;
        }

        private long getMaxEndTime(TaxiRide ride) throws RuntimeException {
            if (!ride.isStart)
                throw new RuntimeException("Can not get start time from END event.");
            return ride.eventTime.plusSeconds(120 * 60).toEpochMilli();
        }

    }
}
