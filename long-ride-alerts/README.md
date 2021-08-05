<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lab: `ProcessFunction` and Timers (Long Ride Alerts)

The goal of the "Long Ride Alerts" exercise is to provide a warning whenever a taxi ride
lasts for more than two hours.

This should be done using the event time timestamps and watermarks that are provided in the data stream.

The stream is out-of-order, and it is possible that the END event for a ride will be processed before
its START event.

A START or END event may be missing, but you may assume there are no duplicated events.

You should eventually clear any state you create.

### Input Data

The input data of this exercise is a `DataStream` of taxi ride events.

### Expected Output

The result of the exercise should be a `DataStream<LONG>` that contains the `rideId` for rides
with a duration that exceeds two hours.

The resulting stream should be printed to standard out.

## Getting Started

> :information_source: Rather than following these links to the sources, you might prefer to open these classes in your IDE.

### Exercise Classes

- Java:  [`org.apache.flink.training.exercises.longrides.LongRidesExercise`](src/main/java/org/apache/flink/training/exercises/longrides/LongRidesExercise.java)
- Scala: [`org.apache.flink.training.exercises.longrides.scala.LongRidesExercise`](src/main/scala/org/apache/flink/training/exercises/longrides/scala/LongRidesExercise.scala)

### Tests

- Java:  [`org.apache.flink.training.exercises.longrides.LongRidesTest`](src/test/java/org/apache/flink/training/exercises/longrides/LongRidesTest.java)
- Scala: [`org.apache.flink.training.exercises.longrides.scala.LongRidesTest`](src/test/scala/org/apache/flink/training/exercises/longrides/scala/LongRidesTest.scala)

## Implementation Hints

<details>
<summary><strong>Overall approach</strong></summary>

This exercise revolves around using a `KeyedProcessFunction` to manage some state and event time timers,
and doing so in a way that works even when the END event for a given `rideId` arrives before the START.
The challenge is figuring out what state and timers to use, and when to set and clear the state (and timers).
It is not enough to simply wait for the END event and calculate the duration, as the END event
may be missing.
</details>

## Documentation

- [ProcessFunction](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html)
- [Working with State](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/index.html)

## After you've completed the exercise

Read the [discussion of the reference solutions](DISCUSSION.md).

## Reference Solutions

Reference solutions:

- Java API:  [`org.apache.flink.training.solutions.longrides.LongRidesSolution`](src/solution/java/org/apache/flink/training/solutions/longrides/LongRidesSolution.java)
- Scala API: [`org.apache.flink.training.solutions.longrides.scala.LongRidesSolution`](src/solution/scala/org/apache/flink/training/solutions/longrides/scala/LongRidesSolution.scala)

-----

[**Back to Labs Overview**](../README.md#lab-exercises)
