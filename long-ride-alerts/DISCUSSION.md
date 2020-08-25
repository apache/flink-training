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

# Lab Discussion: `ProcessFunction` and Timers (Long Ride Alerts)

(Discussion of [Lab: `ProcessFunction` and Timers (Long Ride Alerts)](./))

It would be interesting to test that the solution does not leak state.

A good way to write unit tests for a `KeyedProcessFunction` to check for state retention, etc., is to
use the test harnesses described in the
[documentation on testing](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html#unit-testing-stateful-or-timely-udfs--custom-operators). 

In fact, the reference solutions will leak state in the case where a START event is missing. They also
leak in the case where the alert is generated, but then the END event does eventually arrive (after `onTimer()`
has cleared the matching START event).

This could be addressed either by using [state TTL](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html#state-time-to-live-ttl),
or by using another timer that eventually
clears any remaining state. There is a tradeoff here, however: once that state has been removed,
then if the matching events are not actually missing, but are instead very, very late, they will cause erroneous alerts.

This tradeoff between keeping state indefinitely versus occasionally getting things wrong when events are
exceptionally late is a challenge that is inherent to stateful stream processing.

-----

[**Back to Labs Overview**](../LABS-OVERVIEW.md)
