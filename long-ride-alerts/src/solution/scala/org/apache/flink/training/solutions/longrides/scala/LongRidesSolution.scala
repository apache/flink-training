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

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideSource
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase._
import org.apache.flink.util.Collector

/**
  * Scala reference implementation for the "Long Ride Alerts" exercise of the Flink training in the docs.
  *
  * The goal for this exercise is to emit START events for taxi rides that have not been matched
  * by an END event during the first 2 hours of the ride.
  *
  * Parameters:
  * -input path-to-input-file
  */
object LongRidesSolution {

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", PATH_TO_RIDE_DATA)

    val maxDelay = 60     // events are out of order by max 60 seconds
    val speed = 1800      // events of 30 minutes are served every second

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))

    val longRides = rides
      .keyBy(_.rideId)
      .process(new MatchFunction())

    printOrTest(longRides)

    env.execute("Long Taxi Rides")
  }

  class MatchFunction extends KeyedProcessFunction[Long, TaxiRide, TaxiRide] {
    // keyed, managed state
    // holds an END event if the ride has ended, otherwise a START event
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))

    override def processElement(ride: TaxiRide,
                                context: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
                                out: Collector[TaxiRide]): Unit = {
      val timerService = context.timerService

      if (ride.isStart) {
        // the matching END might have arrived first; don't overwrite it
        if (rideState.value() == null) {
          rideState.update(ride)
        }
      }
      else {
        rideState.update(ride)
      }

      timerService.registerEventTimeTimer(ride.getEventTime + 120 * 60 * 1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#OnTimerContext,
                         out: Collector[TaxiRide]): Unit = {
      val savedRide = rideState.value

      if (savedRide != null && savedRide.isStart) {
        out.collect(savedRide)
      }

      rideState.clear()
    }
  }

}
