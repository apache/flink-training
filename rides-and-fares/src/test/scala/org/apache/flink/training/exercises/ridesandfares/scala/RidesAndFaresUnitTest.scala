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

import org.apache.flink.training.exercises.ridesandfares
import org.apache.flink.training.exercises.common.datatypes.{RideAndFare, TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.testing.ComposedRichCoFlatMapFunction
import org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution

/** The Scala tests extend the Java tests by overriding the composedEnrichmentFunction() method
  * to use the Scala implementations of the exercise and solution.
  */
class RidesAndFaresUnitTest extends ridesandfares.RidesAndFaresUnitTest {

  private val scalaExercise = new RidesAndFaresExercise.EnrichmentFunction
  private val scalaSolution = new RidesAndFaresSolution.EnrichmentFunction

  override def composedEnrichmentFunction
      : ComposedRichCoFlatMapFunction[TaxiRide, TaxiFare, RideAndFare] =
    new ComposedRichCoFlatMapFunction[TaxiRide, TaxiFare, RideAndFare](scalaExercise, scalaSolution)
}
