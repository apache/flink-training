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

import java.util

import org.apache.flink.api.java.tuple
import org.apache.flink.training.exercises.common.datatypes.{TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.ridesandfares
import org.apache.flink.training.exercises.testing.TaxiRideTestBase
import org.apache.flink.training.solutions.ridesandfares.scala.RidesAndFaresSolution

class RidesAndFaresTest extends ridesandfares.RidesAndFaresTest{
  private val scalaExercise: TaxiRideTestBase.Testable = () => RidesAndFaresExercise.main(Array.empty[String])

  @throws[Exception]
  override protected def results(rides: TaxiRideTestBase.TestRideSource, fares: TaxiRideTestBase.TestFareSource): util.List[tuple.Tuple2[TaxiRide, TaxiFare]] = {
    val scalaSolution: TaxiRideTestBase.Testable = () => RidesAndFaresSolution.main(Array.empty[String])
    val tuples: util.List[_] = runApp(rides, fares, new TaxiRideTestBase.TestSink[tuple.Tuple2[TaxiRide, TaxiFare]], scalaExercise, scalaSolution)
    javaTuples(tuples.asInstanceOf[util.List[(TaxiRide, TaxiFare)]])
  }

  private def javaTuples(a: util.List[(TaxiRide, TaxiFare)]): util.ArrayList[tuple.Tuple2[TaxiRide, TaxiFare]] = {
    val javaCopy: util.ArrayList[tuple.Tuple2[TaxiRide, TaxiFare]] = new util.ArrayList[tuple.Tuple2[TaxiRide, TaxiFare]](a.size)
    a.iterator.forEachRemaining((t: (TaxiRide, TaxiFare)) => javaCopy.add(tuple.Tuple2.of(t._1, t._2)))
    javaCopy
  }

}
