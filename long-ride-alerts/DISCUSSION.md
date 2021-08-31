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

# Lab Discussion: `KeyedProcessFunction` and Timers (Long Ride Alerts)

(Discussion of [Lab: `KeyedProcessFunction` and Timers (Long Ride Alerts)](./))

Flaws in the reference solutions:

* The reference solutions leak state in the case where a START event is missing.
* In the case where the END event eventually arrives, but after the timer
has fired and has cleared the matching START event, then a duplicate alert is generated.

A good way to write unit tests for a `KeyedProcessFunction` to check for state retention, etc., is to
use the test harnesses described in the
[documentation on testing](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html#unit-testing-stateful-or-timely-udfs--custom-operators).

These issues could be addressed by keeping some state longer, and then either
using [state TTL](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html#state-time-to-live-ttl),
or another timer, to eventually clear any lingering state.

But regardless of how long we retain the state, we must eventually clear it, and thereafter we would
still run the risk of extremely late events causing incorrect or duplicated results.
This tradeoff between keeping state indefinitely versus occasionally getting things wrong when events are
exceptionally late is a challenge that is inherent to stateful stream processing.

-----

[**Back to Labs Overview**](../README.md#lab-exercises)
