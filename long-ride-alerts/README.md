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

The goal of the "Long Ride Alerts" exercise is to provide a real-time warning whenever a taxi ride
started two hours ago, and is still ongoing.

This should be done using the event time timestamps and watermarks that are provided in the data stream. 

The stream is out-of-order, and it is possible that the END event for a ride will be processed before
its START event. But in such cases, we never care to create an alert, since we do know that the ride
has ended.

### Input Data

The input data of this exercise is a `DataStream` of taxi ride events.

### Expected Output

The goal of this exercise is _not_ to find all rides that lasted for more than two hours, but rather
to create an alert _in real time_ at the moment it becomes known that a ride has been going on for
more than two hours.

The result of the exercise should be a `DataStream<TaxiRide>` that only contains START events of
taxi rides that started two hours earlier, and whose END event hasn't yet arrived.

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

This exercise revolves around using a `ProcessFunction` to manage some keyed state and event time timers, 
and doing so in a way that works even when the END event for a given `rideId` arrives before the START (which can happen). 
The challenge is figuring out what state to keep, and when to set and clear that state.
You will want to use event time timers that fire two hours after an incoming START event, and in the `onTimer()` method, 
collect START events to the output only if a matching END event hasn't yet arrived.
</details>

<details>
<summary><strong>State and timers</strong></summary>

There are many possible solutions for this exercise, but in general it is enough to keep one
`TaxiRide` in state (one `TaxiRide` for each key, or `rideId`). The approach used in the reference solution is to
store whichever event arrives first (the START or the END), and if it's a START event,
create a timer for two hours later. If and when the other event (for the same `rideId`) arrives,
carefully clean things up.

It is possible to arrange this so that if `onTimer()` is called, you are guaranteed that
an alert (i.e., the ride kept in state) should be emitted. Writing the code this way conveniently
puts all of the complex business logic together in one place (in the `processElement()` method).
</details>

## Documentation

- [ProcessFunction](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html)
- [Working with State](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/index.html)

## Reference Solutions

Reference solutions are available at GitHub:

- Java API:  [`org.apache.flink.training.solutions.longrides.LongRidesSolution`](src/solution/java/org/apache/flink/training/solutions/longrides/LongRidesSolution.java)
- Scala API: [`org.apache.flink.training.solutions.longrides.scala.LongRidesSolution`](src/solution/scala/org/apache/flink/training/solutions/longrides/scala/LongRidesSolution.scala)

-----

[**Lab Discussion: `ProcessFunction` and Timers (Long Ride Alerts)**](DISCUSSION.md)

[**Back to Labs Overview**](../LABS-OVERVIEW.md)
