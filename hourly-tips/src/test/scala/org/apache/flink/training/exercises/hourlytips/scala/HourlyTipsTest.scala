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

import java.util

import org.apache.flink.api.java.tuple
import org.apache.flink.training.exercises.hourlytips
import org.apache.flink.training.exercises.testing.TaxiRideTestBase
import org.apache.flink.training.solutions.hourlytips.scala.HourlyTipsSolution

class HourlyTipsTest extends hourlytips.HourlyTipsTest {
  private val scalaExercise: TaxiRideTestBase.Testable = () => HourlyTipsExercise.main(Array.empty[String])

  @throws[Exception]
  override protected def results(source: TaxiRideTestBase.TestFareSource): util.List[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = {
    val scalaSolution: TaxiRideTestBase.Testable = () => HourlyTipsSolution.main(Array.empty[String])
    val tuples: util.List[_] = runApp(source, new TaxiRideTestBase.TestSink[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]], scalaExercise, scalaSolution)
    javaTuples(tuples.asInstanceOf[util.List[(Long, Long, Float)]])
  }

  private def javaTuples(a: util.List[(Long, Long, Float)]): util.ArrayList[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = {
    val javaCopy: util.ArrayList[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = new util.ArrayList[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]](a.size)
    a.iterator.forEachRemaining((t: (Long, Long, Float)) => javaCopy.add(tuple.Tuple3.of(t._1, t._2, t._3)))
    javaCopy
  }

}
