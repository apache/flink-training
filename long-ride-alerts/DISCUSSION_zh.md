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

# 练习讨论: `ProcessFunction` 及定时器（长车程警报）

(关于[练习: `ProcessFunction` 及定时器（长车程警报）](./README_zh.md)的讨论)

### 分析

这些情况值得注意：

* _缺少 START 事件_。 然后 END 事件将被无限期地存储在状态中（这是一个漏洞！）。
* _END 事件丢失_。 计时器将被触发并且状态将被清除（这没关系）。
* _END 事件在计时器触发并清除状态后到达。_ 在这种情况下，END 事件将被无限期地存储在状态中（这是另一个漏洞！）。

这些漏洞可以通过使用 [状态有效期](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl) 或其他计时器，以最终清除残留的状态。

### 底线

不管如何聪明地处理保持什么样的状态，以及选择保持多长时间，我们最终都应该清除它——否则状态将以无限的方式增长。
如果丢失了这些信息，我们将冒着延迟事件导致错误或重复结果的风险。

在永久地保持状态与在事件延迟时偶尔出错之间的权衡是有状态流处理中固有的挑战。

### 如果你想走得更远

对于下列的每一项，添加测试以检查所需的行为。

* 扩展解决方案，使其永远不会泄漏状态。
* 定义事件丢失的含义，检测丢失的 START 和 END 事件，并将一些通知发送到旁路输出。

-----

[**返回练习概述**](../README_zh.md#lab-exercises)
