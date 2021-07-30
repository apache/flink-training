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

package org.apache.flink.training.exercises.longrides.scala

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.longrides
import org.apache.flink.training.exercises.testing.{ComposedPipeline, ExecutablePipeline, TestSink}
import org.apache.flink.training.solutions.longrides.scala.LongRidesSolution

/**
 * The Scala tests extend the Java tests by overriding the longRidesPipeline() method
 * to use the Scala implementations of the exercise and solution.
 */
class LongRidesTest extends longrides.LongRidesTest {
  private val EXERCISE: ExecutablePipeline[TaxiRide, Long] =
    (source: SourceFunction[TaxiRide], sink: TestSink[Long]) => (new LongRidesExercise.LongRidesJob(source, sink)).execute()

  private val SOLUTION: ExecutablePipeline[TaxiRide, Long] =
    (source: SourceFunction[TaxiRide], sink: TestSink[Long]) => (new LongRidesSolution.LongRidesJob(source, sink)).execute()

  override def longRidesPipeline: ComposedPipeline[TaxiRide, Long] =
    new ComposedPipeline[TaxiRide, Long](EXERCISE, SOLUTION)
}
