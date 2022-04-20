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
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.hourlytips
import org.apache.flink.training.exercises.testing.{ComposedPipeline, ExecutablePipeline, TestSink}
import org.apache.flink.training.solutions.hourlytips.scala.HourlyTipsSolution

/** This class uses the java tests to test the scala implementations
  * of the hourly tips exercise and solution. This gets a bit messy because
  * the scala implementations use native scala tuples, which requires converting
  * the list of results to java tuples.
  */
class HourlyTipsTest extends hourlytips.HourlyTipsTest {

  @throws[Exception]
  override def results(
      source: SourceFunction[TaxiFare]
  ): util.List[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = {

    val sink = new TestSink[(Long, Long, Float)]
    val jobResult = hourlyTipsPipeline.execute(source, sink)
    val tuples: util.List[_] = sink.getResults(jobResult)

    javaTuples(tuples.asInstanceOf[util.List[(Long, Long, Float)]])
  }

  private def hourlyTipsPipeline: ComposedPipeline[TaxiFare, (Long, Long, Float)] = {
    val exercise: ExecutablePipeline[TaxiFare, (Long, Long, Float)] =
      (source: SourceFunction[TaxiFare], sink: TestSink[(Long, Long, Float)]) =>
        new HourlyTipsExercise.HourlyTipsJob(source, sink).execute()

    val solution: ExecutablePipeline[TaxiFare, (Long, Long, Float)] =
      (source: SourceFunction[TaxiFare], sink: TestSink[(Long, Long, Float)]) =>
        new HourlyTipsSolution.HourlyTipsJob(source, sink).execute()

    new ComposedPipeline[TaxiFare, (Long, Long, Float)](exercise, solution)
  }

  // The scala pipeline uses scala tuples, while the java pipeline uses Flink's java tuples.
  // This method constructs a list of java tuples from the list of scala tuples passed in.
  private def javaTuples(
      listOfScalaTuples: util.List[(Long, Long, Float)]
  ): util.ArrayList[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] = {

    val listOfJavaTuples
        : util.ArrayList[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]] =
      new util.ArrayList[tuple.Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]](
        listOfScalaTuples.size
      )

    listOfScalaTuples.iterator.forEachRemaining((t: (Long, Long, Float)) =>
      listOfJavaTuples.add(tuple.Tuple3.of(t._1, t._2, t._3))
    )

    listOfJavaTuples
  }

}
