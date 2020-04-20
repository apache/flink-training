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

import java.util

import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.ridecleansing
import org.apache.flink.training.exercises.testing.TaxiRideTestBase
import org.apache.flink.training.solutions.ridecleansing.scala.RideCleansingSolution

class RideCleansingTest extends ridecleansing.RideCleansingTest{
  private val scalaExercise: TaxiRideTestBase.Testable = () => RideCleansingExercise.main(Array.empty[String])

  @throws[Exception]
  override protected def results(source: TaxiRideTestBase.TestRideSource): util.List[TaxiRide] = {
    val scalaSolution: TaxiRideTestBase.Testable = () => RideCleansingSolution.main(Array.empty[String])
    runApp(source, new TaxiRideTestBase.TestSink[TaxiRide], scalaExercise, scalaSolution)
  }

}
