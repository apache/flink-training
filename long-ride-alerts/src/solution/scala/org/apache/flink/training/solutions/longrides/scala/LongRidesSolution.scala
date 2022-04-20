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
            ride.getEventTimeMillis
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

      val firstRideEvent: TaxiRide = rideState.value

      if (firstRideEvent == null) {
        // whatever event comes first, remember it
        rideState.update(ride)

        if (ride.isStart) {
          // we will use this timer to check for rides that have gone on too long and may
          // not yet have an END event (or the END event could be missing)
          context.timerService.registerEventTimeTimer(getTimerTime(ride))
        }
      } else {
        if (ride.isStart) {
          if (rideTooLong(ride, firstRideEvent)) {
            out.collect(ride.rideId)
          }
        } else {
          // the first ride was a START event, so there is a timer unless it has fired
          context.timerService.deleteEventTimeTimer(getTimerTime(firstRideEvent))

          // perhaps the ride has gone on too long, but the timer didn't fire yet
          if (rideTooLong(firstRideEvent, ride)) {
            out.collect(ride.rideId)
          }
        }

        // both events have now been seen, we can clear the state
        // this solution can leak state if an event is missing
        // see DISCUSSION.md for more information
        rideState.clear()
      }
    }

    override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[Long, TaxiRide, Long]#OnTimerContext,
        out: Collector[Long]
    ): Unit = {

      // the timer only fires if the ride was too long
      out.collect(rideState.value().rideId)

      // clearing now prevents duplicate alerts, but will leak state if the END arrives
      rideState.clear()
    }

    private def rideTooLong(startEvent: TaxiRide, endEvent: TaxiRide) =
      Duration
        .between(startEvent.eventTime, endEvent.eventTime)
        .compareTo(Duration.ofHours(2)) > 0

    private def getTimerTime(ride: TaxiRide) = ride.eventTime.toEpochMilli + 2.hours.toMillis
  }

}
