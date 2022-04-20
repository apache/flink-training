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

package org.apache.flink.training.exercises.hourlytips.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.training.exercises.common.utils.MissingSolutionException

/** The Hourly Tips exercise from the Flink training.
  *
  * The task of the exercise is to first calculate the total tips collected by each driver,
  * hour by hour, and then from that stream, find the highest tip total in each hour.
  */
object HourlyTipsExercise {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job = new HourlyTipsJob(new TaxiFareGenerator, new PrintSinkFunction)

    job.execute()
  }

  class HourlyTipsJob(source: SourceFunction[TaxiFare], sink: SinkFunction[(Long, Long, Float)]) {

    /** Create and execute the ride cleansing pipeline.
      */
    @throws[Exception]
    def execute(): JobExecutionResult = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // start the data generator
      val fares: DataStream[TaxiFare] = env.addSource(source)

      // replace this with your solution
      if (true) {
        throw new MissingSolutionException
      }

      // the results should be sent to the sink that was passed in
      // (otherwise the tests won't work)
      // you can end the pipeline with something like this:

      // val hourlyMax = ...
      // hourlyMax.addSink(sink);

      // execute the pipeline and return the result
      env.execute("Hourly Tips")
    }
  }
}
