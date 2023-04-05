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

package org.apache.flink.training.exercises.ridecleansing.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.{GeoUtils, MissingSolutionException}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.TaxiRide

/** The Ride Cleansing exercise from the Flink training.
  *
  * The task of this exercise is to filter a data stream of taxi ride records to keep only
  * rides that both start and end within New York City. The resulting stream should be printed
  * to the standard out.
  */
object RideCleansingExercise {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job = new RideCleansingJob(new TaxiRideGenerator, new PrintSinkFunction)

    job.execute()
  }

  class RideCleansingJob(source: SourceFunction[TaxiRide], sink: SinkFunction[TaxiRide]) {
    /** Creates and executes the ride cleansing pipeline.
      */
    @throws[Exception]
    def execute(): JobExecutionResult = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // set up the pipeline
      env
        .addSource(source)
        .filter(new NYCFilter())
        .addSink(sink)

      // execute the pipeline and return the result
      env.execute()
    }
  }

  /** Keep only those rides and both start and end in NYC. */
  class NYCFilter extends FilterFunction[TaxiRide] {
    override def filter(taxiRide: TaxiRide): Boolean = {
      GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat)
    }
  }
}
