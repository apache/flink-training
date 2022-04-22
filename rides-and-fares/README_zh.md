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

# 练习: 有状态的增强(车程及车费)

本练习的目标是将每次车程的 `TaxiRide` 和 `TaxiFare` 记录连接在一起。

对于每个不同的 `rideId`，恰好有三个事件：

1. `TaxiRide` START 事件
1. `TaxiRide` END 事件
1. 一个 `TaxiFare` 事件（其时间戳恰好与开始时间匹配）

最终的结果应该是 `DataStream<RideAndFare>`，每个不同的 `rideId` 都产生一个 `RideAndFare` 记录。
每个 `RideAndFare` 都应该将某个 `rideId` 的 `TaxiRide` START 事件与其匹配的 `TaxiFare` 配对。

### 输入数据

在练习中，你将使用两个数据流，一个使用由 `TaxiRideSource` 生成的 `TaxiRide` 事件，另一个使用由 `TaxiFareSource` 生成的 `TaxiFare` 事件。
有关如何使用这些流生成器的信息，请参阅 [使用出租车数据流](../README_zh.md#using-the-taxi-data-streams)。

### 期望输出

所希望的结果是一个 `RideAndFare` 记录的数据流，每个不同的 `rideId` 都有一条这样的记录。
本练习设置为忽略 END 事件，你应该连接每次乘车的 START 事件及其相应的车费事件。

一旦具有了相互关联的车程和车费事件，你可以使用 `new RideAndFare(ride, fare)` 方法为输出流创建所需的对象。

流将会被打印到标准输出。

## 入门指南

> :information_source: 最好在 IDE 的 flink-training 项目中找到这些类，而不是使用本节中源文件的链接。

### 练习相关类

- Java:  [`org.apache.flink.training.exercises.ridesandfares.RidesAndFaresExercise`](src/main/java/org/apache/flink/training/exercises/ridesandfares/RidesAndFaresExercise.java)
- Scala: [`org.apache.flink.training.exercises.ridesandfares.scala.RidesAndFaresExercise`](src/main/scala/org/apache/flink/training/exercises/ridesandfares/scala/RidesAndFaresExercise.scala)

### 集成测试

- Java:  [`org.apache.flink.training.exercises.ridesandfares.RidesAndFaresIntegrationTest`](src/test/java/org/apache/flink/training/exercises/ridesandfares/RidesAndFaresIntegrationTest.java)
- Scala: [`org.apache.flink.training.exercises.ridesandfares.scala.RidesAndFaresIntegrationTest`](src/test/scala/org/apache/flink/training/exercises/ridesandfares/scala/RidesAndFaresIntegrationTest.scala)

## 实现提示

<details>
<summary><strong>程序结构</strong></summary>

可以使用 `RichCoFlatMap` 来实现连接操作。请注意，你无法控制每个 rideId 的车程和车费记录的到达顺序，因此需要存储其中一个事件，直到与其匹配的另一事件到达。
此时你可以创建并发出 `RideAndFare` 以将两条记录连接在一起。
</details>

<details>
<summary><strong>使用状态</strong></summary>

应该使用由 Flink 管理的、按键值分割(keyed)的状态来缓冲想要暂时保存的数据，直到匹配事件到达，并确保在不再需要时清除该状态。
</details>

## 讨论

出于练习的目的，可以假设 START 和 fare 事件完美配对。
但是在现实世界的应用程序中，你应该担心每当一个事件丢失时，同一个 `rideId` 的另一个事件的状态将被永远保持。
在 [稍后的练习](../long-ride-alerts/README_zh.md) 中，我们将看到 `ProcessFunction` 和定时器，它们将有助于处理这样的情况。

## 相关文档

- [使用状态](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/fault-tolerance/state)

## 参考解决方案

项目中提供了参考解决方案：

- Java:  [`org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution`](src/solution/java/org/apache/flink/training/solutions/ridesandfares/RidesAndFaresSolution.java)
- Scala: [`org.apache.flink.training.solutions.ridesandfares.scala.RidesAndFaresSolution`](src/solution/scala/org/apache/flink/training/solutions/ridesandfares/scala/RidesAndFaresSolution.scala)

-----

[**返回练习概述**](../README_zh.md#lab-exercises)
