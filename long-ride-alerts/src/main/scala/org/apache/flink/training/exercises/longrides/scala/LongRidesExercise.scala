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

package org.apache.flink.training.exercises.longrides.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.MissingSolutionException
import org.apache.flink.util.Collector

import java.time.Duration

/** The "Long Ride Alerts" exercise.
  *
  * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
  * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
  *
  * <p>You should eventually clear any state you create.
  */
object LongRidesExercise {

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

      // execute the pipeline and return the result
      env.execute("Long Taxi Rides")
    }

  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job = new LongRidesJob(new TaxiRideGenerator, new PrintSinkFunction)

    job.execute()
  }

  class AlertFunction extends KeyedProcessFunction[Long, TaxiRide, Long] {

    override def open(parameters: Configuration): Unit = {
      throw new MissingSolutionException()
    }

    override def processElement(
        ride: TaxiRide,
        context: KeyedProcessFunction[Long, TaxiRide, Long]#Context,
        out: Collector[Long]
    ): Unit = {}

    override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[Long, TaxiRide, Long]#OnTimerContext,
        out: Collector[Long]
    ): Unit = {}

  }

}
