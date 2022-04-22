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

# 练习: `ProcessFunction` 及定时器（长车程警报）

“长车程警报”练习的目标是对于持续超过两个小时的出租车车程发出警报。

这应该使用数据流中提供的事件时间时间戳和水位线来完成。

流是无序的，并且可能会在其 START 事件之前处理车程的 END 事件。

END 事件可能会丢失，但你可以假设没有重复的事件，也没有丢失的 START 事件。

仅仅等待 END 事件并计算持续时间是不够的，因为我们希望尽快收到关于长车程的警报。

最终应该清除创建的任何状态。

### 输入数据

输入数据是出租车乘车事件的 `DataStream`。

### 期望输出

所希望的结果应该是一个 `DataStream<LONG>`，其中包含持续时间超过两小时的车程的 `rideId`。

结果流应打印到标准输出。

## 入门指南

> :information_source: 最好在 IDE 的 flink-training 项目中找到这些类，而不是使用本节中源文件的链接。
>
### 练习相关类

- Java:  [`org.apache.flink.training.exercises.longrides.LongRidesExercise`](src/main/java/org/apache/flink/training/exercises/longrides/LongRidesExercise.java)
- Scala: [`org.apache.flink.training.exercises.longrides.scala.LongRidesExercise`](src/main/scala/org/apache/flink/training/exercises/longrides/scala/LongRidesExercise.scala)

### 单元测试

- Java:  [`org.apache.flink.training.exercises.longrides.LongRidesUnitTest`](src/test/java/org/apache/flink/training/exercises/longrides/LongRidesUnitTest.java)
- Scala: [`org.apache.flink.training.exercises.longrides.scala.LongRidesUnitTest`](src/test/scala/org/apache/flink/training/exercises/longrides/scala/LongRidesUnitTest.scala)

### 集成测试

- Java:  [`org.apache.flink.training.exercises.longrides.LongRidesIntegrationTest`](src/test/java/org/apache/flink/training/exercises/longrides/LongRidesIntegrationTest.java)
- Scala: [`org.apache.flink.training.exercises.longrides.scala.LongRidesIntegrationTest`](src/test/scala/org/apache/flink/training/exercises/longrides/scala/LongRidesIntegrationTest.scala)

## 实现提示

<details>
<summary><strong>整体方案</strong></summary>

这个练习围绕着使用 `KeyedProcessFunction` 来管理一些状态和事件时间计时器，
使用这种方法即使在给定 `rideId` 的 END 事件在 START 之前到达时也能正常工作。
挑战在于弄清楚要使用什么状态和计时器，以及何时设置和清除状态（和计时器）。
</details>

## 相关文档

- [ProcessFunction](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/operators/process_function)
- [使用状态](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/fault-tolerance/state)

## 完成练习后

阅读[参考解决方案的讨论](DISCUSSION_zh.md).

## 参考解决方案

项目中提供了参考解决方案：

- Java API:  [`org.apache.flink.training.solutions.longrides.LongRidesSolution`](src/solution/java/org/apache/flink/training/solutions/longrides/LongRidesSolution.java)
- Scala API: [`org.apache.flink.training.solutions.longrides.scala.LongRidesSolution`](src/solution/scala/org/apache/flink/training/solutions/longrides/scala/LongRidesSolution.scala)

-----

[**练习讨论: `ProcessFunction` 及定时器（长车程警报）**](DISCUSSION_zh.md)

[**返回练习概述**](../README_zh.md#lab-exercises)
