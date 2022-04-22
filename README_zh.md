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

# Apache Flink 实践练习

与文档中实践练习内容相关的练习。

## 目录

[**设置开发环境**](#set-up-your-development-environment)

1. [软件要求](#software-requirements)
1. [克隆并构建 flink-training 项目](#clone-and-build-the-flink-training-project)
1. [将 flink-training 项目导入 IDE](#import-the-flink-training-project-into-your-ide)

[**使用出租车数据流(taxi data stream)**](#using-the-taxi-data-streams)

1. [出租车车程(taxi ride)事件结构](#schema-of-taxi-ride-events)
1. [出租车费用(taxi fare)事件结构](#schema-of-taxi-fare-events)

[**如何做练习**](#how-to-do-the-lab-exercises)

1. [了解数据](#learn-about-the-data)
2. [在 IDE 中运行和调试 Flink 程序](#run-and-debug-flink-programs-in-your-ide)
3. [练习、测试及解决方案](#exercises-tests-and-solutions)

[**练习**](#lab-exercises)

[**提交贡献**](#contributing)

[**许可证**](#license)

<a name="set-up-your-development-environment"></a>

## 设置开发环境

你需要设置便于进行开发、调试并运行实践练习的示例和解决方案的环境。

<a name="software-requirements"></a>

### 软件要求

Linux、OS X 和 Windows 均可作为 Flink 程序和本地执行的开发环境。 Flink 开发设置需要以下软件，它们应该安装在系统上：

- Git
- Java 8 或者 Java 11 版本的 JDK (JRE不满足要求；目前不支持其他版本的Java)
- 支持 Gradle 的 Java (及/或 Scala) 开发IDE
    - 推荐使用 [IntelliJ](https://www.jetbrains.com/idea/), 但 [Eclipse](https://www.eclipse.org/downloads/) 或 [Visual Studio Code](https://code.visualstudio.com/) (安装 [Java extension pack](https://code.visualstudio.com/docs/java/java-tutorial) 插件) 也可以用于Java环境
    - 为了使用 Scala, 需要使用 IntelliJ (及其 [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala/) 插件)

> **:information_source: Windows 用户须知：** 实践说明中提供的 shell 命令示例适用于 UNIX 环境。
> 您可能会发现值得在 Windows 环境中设置 cygwin 或 WSL。对于开发 Flink 作业(jobs)，Windows工作的相当好：可以在单机上运行 Flink 集群、提交作业、运行 webUI 并在IDE中执行作业。

<a name="clone-and-build-the-flink-training-project"></a>

### 克隆并构建 flink-training 项目

`flink-training` 仓库包含编程练习的习题、测试和参考解决方案。

> **:information_source: 仓库格局:** 本仓库有几个分支，分别指向不同的 Apache Flink 版本，类似于 [apache/flink](https://github.com/apache/flink) 仓库：
> - 每个 Apache Flink 次要版本的发布分支，例如 `release-1.10`，和
> - 一个指向当前 Flink 版本的 `master` 分支（不是 `flink:master`！）
>
> 如果想在当前 Flink 版本以外的版本上工作，请务必签出相应的分支。

从 GitHub 克隆出 `flink-training` 仓库，导航到本地项目仓库并构建它：

```bash
git clone https://github.com/apache/flink-training.git
cd flink-training
./gradlew test shadowJar
```

如果是第一次构建，将会下载此 Flink 练习项目的所有依赖项。这通常需要几分钟时间，但具体取决于互联网连接速度。

如果所有测试都通过并且构建成功，这说明你的实践练习已经开了一个好头。

<details>
<summary><strong>:cn: 中国用户: 点击这里了解如何使用本地 Maven 镜像。</strong></summary>

如果你在中国，我们建议将 Maven 存储库配置为使用镜像。 可以通过在 [`build.gradle`](build.gradle) 文件中取消注释此部分来做到这一点：

```groovy
    repositories {
        // for access from China, you may need to uncomment this line
        maven { url 'https://maven.aliyun.com/repository/public/' }
        mavenCentral()
        maven {
            url "https://repository.apache.org/content/repositories/snapshots/"
            mavenContent {
                snapshotsOnly()
            }
        }
    }
```
</details>

<details>
<summary><strong>启用 Scala (可选)</strong></summary>
这个项目中的练习也可以使用 Scala ，但由于非 Scala 用户报告的一些问题，我们决定默认禁用 Scala。
可以通过以下的方法修改 `gradle.properties` 文件以重新启用所有 Scala 练习和解决方案：

[`gradle.properties`](gradle.properties) 文件如下：

```properties
#...

# Scala exercises can be enabled by setting this to true
org.gradle.project.enable_scala = true
```

如果需要，还可以选择性地在单个子项目中应用该插件。
</details>

<a name="import-the-flink-training-project-into-your-ide"></a>

### 将 flink-training 项目导入IDE

本项目应作为 gradle 项目导入到IDE中。

然后应该可以打开 [`RideCleansingTest`](ride-cleansing/src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingTest.java) 并运行此测试。

> **:information_source: Scala 用户须知:** 需要将 IntelliJ 与 JetBrains Scala 插件一起使用，并且需要将 Scala 2.12 SDK 添加到项目结构的全局库部分以及工作模块中。
> 当打开 Scala 文件时，IntelliJ 会要求提供后者(JetBrains Scala 插件)。
> 请注意 Scala 2.12.8 及以上版本不受支持 (详细信息参见 [Flink Scala Versions](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/project-configuration/#scala-versions))!

<a name="using-the-taxi-data-streams"></a>

## 使用出租车数据流(taxi data stream)

练习中使用数据[生成器(generators)](common/src/main/java/org/apache/flink/training/exercises/common/sources)产生模拟的事件流。
该数据的灵感来自[纽约市出租车与豪华礼车管理局(New York City Taxi & Limousine Commission)](http://www.nyc.gov/html/tlc/html/home/home.shtml)
的公开[数据集](https://uofi.app.box.com/NYCtaxidata)中有关纽约市出租车的车程情况。

<a name="schema-of-taxi-ride-events"></a>

### 出租车车程(taxi ride)事件结构

出租车数据集包含有关纽约市个人出租车的车程信息。

每次车程都由两个事件表示：行程开始(trip start)和行程结束(trip end)。

每个事件都由十一个字段组成：

```
rideId         : Long      // 每次车程的唯一id
taxiId         : Long      // 每一辆出租车的唯一id
driverId       : Long      // 每一位司机的唯一id
isStart        : Boolean   // 行程开始事件为 TRUE， 行程结束事件为 FALSE
eventTime      : Instant   // 事件的时间戳
startLon       : Float     // 车程开始位置的经度
startLat       : Float     // 车程开始位置的维度
endLon         : Float     // 车程结束位置的经度
endLat         : Float     // 车程结束位置的维度
passengerCnt   : Short     // 乘车人数
```

<a name="schema-of-taxi-fare-events"></a>

### 出租车车费(taxi fare)事件结构

还有一个包含与车程相关费用的数据集，它具有以下字段：

```
rideId         : Long      // 每次车程的唯一id
taxiId         : Long      // 每一辆出租车的唯一id
driverId       : Long      // 每一位司机的唯一id
startTime      : Instant   // 车程开始时间
paymentType    : String    // 现金(CASH)或刷卡(CARD)
tip            : Float     // 小费
tolls          : Float     // 过路费
totalFare      : Float     // 总计车费
```

<a name="how-to-do-the-lab-exercises"></a>

## 如何做练习

在实践课程中，你将使用各种 Flink API 实现 Flink 程序。

以下步骤将指导你完成使用提供的数据流、实现第一个 Flink 流程序以及在 IDE 中执行程序的过程。

我们假设你已根据 [设置指南](#set-up-your-development-environment) 准备好了开发环境。

<a name="learn-about-the-data"></a>

### 了解数据

最初的一组练习都是基于有关出租车车程和出租车车费的事件数据流。这些流由从输入文件读取数据的源函数产生。
参见 [说明](#using-the-taxi-data-streams) 以了解如何使用它们。

<a name="run-and-debug-flink-programs-in-your-ide"></a>

### 在 IDE 中运行和调试 Flink 程序

Flink 程序可以在 IDE 中执行和调试。这显著地简化了开发过程，并可提供类似于使用任何其他 Java（或 Scala）应用程序的体验。

要在 IDE 中启动 Flink 程序，请运行它的 `main()` 方法。在后台，执行环境将在同一进程中启动本地 Flink 实例。因此，可以在代码中放置断点并对其进行调试。

如果 IDE 已导入 `flink-training` 项目，则可以通过以下方式运行（或调试）流式作业：

- 在 IDE 中打开 `org.apache.flink.training.examples.ridecount.RideCountExample` 类
- 使用 IDE 运行（或调试）`RideCountExample` 类的`main()` 方法

<a name="exercises-tests-and-solutions"></a>

### 练习、测试及解决方案

每一项练习都包括：
- 一个 `...Exercise` 类，其中包含运行所需地大多数样板代码
- 一个 JUnit 测试类（`...Test`），其中包含一些针对实现的测试
- 具有完整解决方案的 `...Solution` 类

所有练习、测试和解决方案类都有 Java 和 Scala 版本。 它们都可以在 IntelliJ 中运行。

> **:information_source: 注意:** 只要 `...Exercise` 类抛出 `MissingSolutionException` 异常，那么所提供的 JUnit 测试类将忽略该失败并转而验证已提供的参考解决方案实现的正确性。

你可以使用 `gradlew` 命令运行练习、解决方案和测试。

运行测试：

```bash
./gradlew test
./gradlew :<subproject>:test
```

对于 Java/Scala 练习和解决方案，我们提供了可以获取清单的特殊任务：

```bash
./gradlew printRunTasks
```

:point_down: 至此，你已准备好开始进行练习。 :point_down:

<a name="lab-exercises"></a>

## 练习

1. [过滤流(车程清理)](ride-cleansing/README_zh.md)
1. [有状态的增强(车程及车费)](rides-and-fares/README_zh.md)
1. [窗口分析(每小时小费)](hourly-tips/README_zh.md)
    - [练习](hourly-tips/README_zh.md)
    - [讨论](hourly-tips/DISCUSSION_zh.md)
1. [`ProcessFunction` 及定时器(长车程警报)](long-ride-alerts/README_zh.md)
    - [练习](long-ride-alerts/README_zh.md)
    - [讨论](long-ride-alerts/DISCUSSION.md)

<a name="contributing"></a>

## 提交贡献

如果你想为此仓库做出贡献或添加新练习，请阅读 [提交贡献](CONTRIBUTING.md) 指南。

<a name="license"></a>

## 许可证

本仓库中的代码基于 [Apache Software License 2](LICENSE) 许可证。
