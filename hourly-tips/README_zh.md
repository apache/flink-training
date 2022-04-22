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

# 练习: 窗口分析 (每小时小费)

“每小时小费”练习的任务是确定每小时赚取最多小费的司机。
最简单的方法是通过两个步骤来解决这个问题：首先使用一个小时长的窗口来计算每个司机在一小时内的总小费，然后从该窗口结果流中找到每小时总小费最多的司机。

请注意，该程序应使用事件时间（event time）。

### 输入数据

本练习的输入数据是由[出租车车费流生成器](../README_zh.md#using-the-taxi-data-streams)生成的 `TaxiFare` 事件流。

`TaxiFareGenerator` 用时间戳和水位线（watermark）注解生成的 `DataStream<TaxiFare>`。
因此，无需提供自定义的时间戳和水印分配器即可正确使用事件时间。

### 期望输出

所希望的结果是每小时产生一个 `Tuple3<Long, Long, Float>` 记录的数据流。
这个记录（`Tuple3<Long, Long, Float>`）应包含该小时结束时的时间戳（对应三元组的第一个元素）、
该小时内获得小费最多的司机的 driverId（对应三元组的第二个元素）以及他的实际小费总数（对应三元组的第三个元素））。

结果流应打印到标准输出。

## 入门指南

> :information_source: 最好在 IDE 的 flink-training 项目中找到这些类，而不是使用本节中源文件的链接。

### 练习相关类

- Java:  [`org.apache.flink.training.exercises.hourlytips.HourlyTipsExercise`](src/main/java/org/apache/flink/training/exercises/hourlytips/HourlyTipsExercise.java)
- Scala: [`org.apache.flink.training.exercises.hourlytips.scala.HourlyTipsExercise`](src/main/scala/org/apache/flink/training/exercises/hourlytips/scala/HourlyTipsExercise.scala)

### 测试

- Java:  [`org.apache.flink.training.exercises.hourlytips.HourlyTipsTest`](src/test/java/org/apache/flink/training/exercises/hourlytips/HourlyTipsTest.java)
- Scala: [`org.apache.flink.training.exercises.hourlytips.scala.HourlyTipsTest`](src/test/scala/org/apache/flink/training/exercises/hourlytips/scala/HourlyTipsTest.scala)

## 实现提示

<details>
<summary><strong>程序结构</strong></summary>

请注意，可以将一组时间窗口逐个级联，只要时间帧兼容（第二组窗口的持续时间需要是第一组的倍数）。
因此，首先可以得到一个由 `driverId` 键值分隔的具有一小时窗口的初始数据集，并使用它来创建一个 `(endOfHourTimestamp，driverId，totalTips)` 流。
然后使用另一个一小时窗口（该窗口不是用键值分隔的），从第一个窗口中查找具有最大 `totalTips` 的记录。
</details>

## 相关文档

- [窗口](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/operators/windows)
- [参阅窗口聚合操作章节](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/operators/overview/#datastream-transformations)

## 参考解决方案

项目中提供了参考解决方案：

- Java:  [`org.apache.flink.training.solutions.hourlytips.HourlyTipsSolution`](src/solution/java/org/apache/flink/training/solutions/hourlytips/HourlyTipsSolution.java)
- Scala: [`org.apache.flink.training.solutions.hourlytips.scala.HourlyTipsSolution`](src/solution/scala/org/apache/flink/training/solutions/hourlytips/scala/HourlyTipsSolution.scala)

-----

[**练习讨论: 窗口分析 (每小时小费)**](DISCUSSION_zh.md)

[**返回练习概述**](../README_zh.md#lab-exercises)
