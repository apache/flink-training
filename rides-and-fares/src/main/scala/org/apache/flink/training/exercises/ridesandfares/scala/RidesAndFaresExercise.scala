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

package org.apache.flink.training.exercises.ridesandfares.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.{RideAndFare, TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.training.exercises.common.utils.MissingSolutionException
import org.apache.flink.util.Collector

/** The Stateful Enrichment exercise from the Flink training.
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  */
object RidesAndFaresExercise {

  class RidesAndFaresJob(
      rideSource: SourceFunction[TaxiRide],
      fareSource: SourceFunction[TaxiFare],
      sink: SinkFunction[RideAndFare]
  ) {

    def execute(): JobExecutionResult = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val rides = env
        .addSource(rideSource)
        .filter { ride =>
          ride.isStart
        }
        .keyBy { ride =>
          ride.rideId
        }

      val fares = env
        .addSource(fareSource)
        .keyBy { fare =>
          fare.rideId
        }

      rides
        .connect(fares)
        .flatMap(new EnrichmentFunction())
        .addSink(sink)

      env.execute()
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job =
      new RidesAndFaresJob(new TaxiRideGenerator, new TaxiFareGenerator, new PrintSinkFunction)

    job.execute()
  }

  class EnrichmentFunction() extends RichCoFlatMapFunction[TaxiRide, TaxiFare, RideAndFare] {

    override def open(parameters: Configuration): Unit = {
      throw new MissingSolutionException()
    }

    override def flatMap1(ride: TaxiRide, out: Collector[RideAndFare]): Unit = {
      throw new MissingSolutionException()
    }

    override def flatMap2(fare: TaxiFare, out: Collector[RideAndFare]): Unit = {
      throw new MissingSolutionException()
    }
  }

}
