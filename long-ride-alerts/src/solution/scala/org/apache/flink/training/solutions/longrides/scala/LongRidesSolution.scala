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

import scala.concurrent.duration._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase._
import org.apache.flink.util.Collector

/**
  * Scala reference implementation for the "Long Ride Alerts" exercise of the Flink training in the docs.
  *
  * The goal for this exercise is to emit START events for taxi rides that have not been matched
  * by an END event during the first 2 hours of the ride.
  *
  */
object LongRidesSolution {

  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))

    val longRides = rides
      .keyBy(_.rideId)
      .process(new MatchFunction())

    printOrTest(longRides)

    env.execute("Long Taxi Rides")
  }

  class MatchFunction extends KeyedProcessFunction[Long, TaxiRide, TaxiRide] {
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("ride event", classOf[TaxiRide]))

    override def processElement(ride: TaxiRide,
                                context: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
                                out: Collector[TaxiRide]): Unit = {

      val previousRideEvent = rideState.value()

      if (previousRideEvent == null) {
        rideState.update(ride)
        if (ride.isStart) {
          context.timerService().registerEventTimeTimer(getTimerTime(ride))
        }
      } else {
        if (!ride.isStart) {
          // it's an END event, so event saved was the START event and has a timer
          // the timer hasn't fired yet, and we can safely kill the timer
          context.timerService().deleteEventTimeTimer(getTimerTime(previousRideEvent))
        }
        // both events have now been seen, we can clear the state
        rideState.clear()
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#OnTimerContext,
                         out: Collector[TaxiRide]): Unit = {

      // if we get here, we know that the ride started two hours ago, and the END hasn't been processed
      out.collect(rideState.value())
      rideState.clear()
    }

    private def getTimerTime(ride: TaxiRide) = {
      ride.startTime.toEpochMilli + 2.hours.toMillis
    }
  }

}
