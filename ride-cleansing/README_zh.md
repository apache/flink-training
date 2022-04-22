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

# 练习: 过滤流(车程清理)

如果尚未设置 Flink 开发环境，请参阅[指南](../README_zh.md)。
有关练习的总体介绍，请参阅[如何做练习](../README_zh.md#how-to-do-the-labs)。

"出租车车程清理"练习的任务是通过删除在纽约市以外开始或结束的车程来清理一系列的 `TaxiRide` 事件。

`GeoUtils` 实用程序类提供了一个静态方法 `isInNYC(float lon, float lat)` 来检查某个位置是否在纽约市区域内。

### 输入数据

此练习基于 `TaxiRide` 事件流，如[使用出租车数据流](../README.md#using-the-taxi-data-streams)中所述。

### 期望输出

练习的结果应该是一个 `DataStream<TaxiRide>`，它只包含在 `GeoUtils.isInNYC()` 定义的纽约市地区开始和结束的出租车车程事件。

结果流应打印到标准输出。

## 入门指南

> :information_source: 最好在 IDE 的 flink-training 项目中找到这些类，而不是使用本节中源文件的链接。
> IntelliJ 和 Eclipse 都可以轻松搜索和导航到类和文件。对于 IntelliJ，请参阅[搜索帮助](https://www.jetbrains.com/help/idea/searching-everywhere.html)，或者只需按 Shift 键两次，然后继续输入类似 `RideCleansing` 的内容，接着从弹出的选项中选择。

### 练习相关类

本练习使用以下类：

- Java:  [`org.apache.flink.training.exercises.ridecleansing.RideCleansingExercise`](src/main/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingExercise.java)
- Scala: [`org.apache.flink.training.exercises.ridecleansing.scala.RideCleansingExercise`](src/main/scala/org/apache/flink/training/exercises/ridecleansing/scala/RideCleansingExercise.scala)

### 测试

练习的测试位于：

- Java:  [`org.apache.flink.training.exercises.ridecleansing.RideCleansingIntegrationTest`](src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingIntegrationTest.java)
- Java:  [`org.apache.flink.training.exercises.ridecleansing.RideCleansingUnitTest`](src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingUnitTest.java)
- Scala: [`org.apache.flink.training.exercises.ridecleansing.scala.RideCleansingIntegrationTest`](src/test/scala/org/apache/flink/training/exercises/ridecleansing/scala/RideCleansingIntegrationTest.scala)
- Scala: [`org.apache.flink.training.exercises.ridecleansing.scala.RideCleansingUnitTest`](src/test/scala/org/apache/flink/training/exercises/ridecleansing/scala/RideCleansingUnitTest.scala)

像大多数练习一样，在某些时候，`RideCleansingExercise` 类会抛出异常

```java
throw new MissingSolutionException();
```

一旦删除此行，测试将会失败，直到你提供有效的解决方案。你也可能想先尝试一些明显错误的代码，例如

```java
return false;
```

如此验证这些错误代码确实可导致测试失败，然后可以向着正确的方向实现适当的解决方案。

## 实现提示

<details>
<summary><strong>过滤事件</strong></summary>

Flink 的 DataStream API 提供了一个 `DataStream.filter(FilterFunction)` 转换函数来过滤数据流中的事件。
可以在 `FilterFunction` 中调用 `GeoUtils.isInNYC()` 函数来检查某个位置是否在纽约市地区。
过滤器应检查每次车程的起点和终点。
</details>

## 相关文档

- [DataStream API](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/overview)
- [Flink JavaDocs](https://nightlies.apache.org/flink/flink-docs-stable/api/java)

## 参考解决方案

Reference solutions are available in this project:

- Java:  [`org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution`](src/solution/java/org/apache/flink/training/solutions/ridecleansing/RideCleansingSolution.java)
- Scala: [`org.apache.flink.training.solutions.ridecleansing.scala.RideCleansingSolution`](src/solution/scala/org/apache/flink/training/solutions/ridecleansing/scala/RideCleansingSolution.scala)

-----

[**返回练习概述**](../README_zh.md#lab-exercises)
