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

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.training.exercises.common.datatypes.{RideAndFare, TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.ridesandfares
import org.apache.flink.training.exercises.testing.{
  ComposedTwoInputPipeline,
  ExecutableTwoInputPipeline,
  TestSink
}
import org.apache.flink.training.solutions.ridesandfares.scala.RidesAndFaresSolution

/** The Scala tests extend the Java tests by overriding the ridesAndFaresPipeline() method
  * to use the Scala implementations of the exercise and solution.
  */
class RidesAndFaresIntegrationTest extends ridesandfares.RidesAndFaresIntegrationTest {

  private val EXERCISE: ExecutableTwoInputPipeline[TaxiRide, TaxiFare, RideAndFare] =
    (
        rides: SourceFunction[TaxiRide],
        fares: SourceFunction[TaxiFare],
        sink: TestSink[RideAndFare]
    ) => new RidesAndFaresExercise.RidesAndFaresJob(rides, fares, sink).execute()

  private val SOLUTION: ExecutableTwoInputPipeline[TaxiRide, TaxiFare, RideAndFare] =
    (
        rides: SourceFunction[TaxiRide],
        fares: SourceFunction[TaxiFare],
        sink: TestSink[RideAndFare]
    ) => new RidesAndFaresSolution.RidesAndFaresJob(rides, fares, sink).execute()

  override def ridesAndFaresPipeline: ComposedTwoInputPipeline[TaxiRide, TaxiFare, RideAndFare] =
    new ComposedTwoInputPipeline[TaxiRide, TaxiFare, RideAndFare](EXERCISE, SOLUTION)

}
