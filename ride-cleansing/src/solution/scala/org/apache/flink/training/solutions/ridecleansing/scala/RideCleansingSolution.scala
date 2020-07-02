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

package org.apache.flink.training.solutions.ridecleansing.scala

import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase._
import org.apache.flink.training.exercises.common.utils.{ExerciseBase, GeoUtils}

/**
 * Scala reference implementation for the "Ride Cleansing" exercise of the Flink training in the docs.
 *
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed to the
 * standard out.
 *
 */
object RideCleansingSolution {

  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    // get the taxi ride data stream
    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))

    val filteredRides = rides
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    // print the filtered stream
    printOrTest(filteredRides)

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }

}
