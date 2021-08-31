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
package org.apache.flink.training.solutions.longrides.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.util.Collector

import java.time.Duration
import scala.concurrent.duration._

/** Scala solution for the "Long Ride Alerts" exercise.
  *
  * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
  * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
  *
  * <p>You should eventually clear any state you create.
  */
object LongRidesSolution {

  class LongRidesJob(source: SourceFunction[TaxiRide], sink: SinkFunction[Long]) {

    /** Creates and executes the ride cleansing pipeline.
      */
    @throws[Exception]
    def execute(): JobExecutionResult = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // start the data generator
      val rides = env.addSource(source)

      // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
      val watermarkStrategy = WatermarkStrategy
        .forBoundedOutOfOrderness[TaxiRide](Duration.ofSeconds(60))
        .withTimestampAssigner(new SerializableTimestampAssigner[TaxiRide] {
          override def extractTimestamp(ride: TaxiRide, streamRecordTimestamp: Long): Long =
            ride.getEventTime
        })

      // create the pipeline
      rides
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .keyBy(_.rideId)
        .process(new AlertFunction())
        .addSink(sink)

      // execute the pipeline
      env.execute("Long Taxi Rides")
    }

  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job = new LongRidesJob(new TaxiRideGenerator, new PrintSinkFunction)

    job.execute()
  }

  class AlertFunction extends KeyedProcessFunction[Long, TaxiRide, Long] {
    private var rideState: ValueState[TaxiRide] = _

    override def open(parameters: Configuration): Unit = {
      rideState = getRuntimeContext.getState(
        new ValueStateDescriptor[TaxiRide]("ride event", classOf[TaxiRide])
      )
    }

    override def processElement(
        ride: TaxiRide,
        context: KeyedProcessFunction[Long, TaxiRide, Long]#Context,
        out: Collector[Long]
    ): Unit = {

      val firstRideEvent = rideState.value()

      if (firstRideEvent == null) {
        rideState.update(ride)
        if (ride.isStart) {
          context.timerService.registerEventTimeTimer(getTimerTime(ride))
        } else if (rideTooLong(ride)) {
          out.collect(ride.rideId)
        }
      } else {
        if (ride.isStart) {
          // There's nothing to do but clear the state (which is done below).
        } else {
          // There may be a timer that hasn't fired yet.
          context.timerService.deleteEventTimeTimer(getTimerTime(firstRideEvent))

          // It could be that the ride has gone on too long, but the timer hasn't fired yet.
          if (rideTooLong(ride)) {
            out.collect(ride.rideId)
          }
        }
        // Both events have now been seen, we can clear the state.
        rideState.clear()
      }
    }

    override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[Long, TaxiRide, Long]#OnTimerContext,
        out: Collector[Long]
    ): Unit = {

      // The timer only fires if the ride was too long.
      out.collect(rideState.value().rideId)
      rideState.clear()
    }

    private def rideTooLong(rideEndEvent: TaxiRide) =
      Duration
        .between(rideEndEvent.startTime, rideEndEvent.endTime)
        .compareTo(Duration.ofHours(2)) > 0

    private def getTimerTime(ride: TaxiRide) = ride.startTime.toEpochMilli + 2.hours.toMillis
  }

}
