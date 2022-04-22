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

[中文版](./README_zh.md)

# Apache Flink Training Exercises

Exercises that accompany the training content in the documentation.

## Table of Contents

[**Set up your development environment**](#set-up-your-development-environment)

1. [Software requirements](#software-requirements)
1. [Clone and build the flink-training project](#clone-and-build-the-flink-training-project)
1. [Import the flink-training project into your IDE](#import-the-flink-training-project-into-your-ide)

[**Use the taxi data streams**](#using-the-taxi-data-streams)

1. [Schema of taxi ride events](#schema-of-taxi-ride-events)
1. [Schema of taxi fare events](#schema-of-taxi-fare-events)

[**How to do the lab exercises**](#how-to-do-the-lab-exercises)

1. [Learn about the data](#learn-about-the-data)
1. [Run and debug Flink programs in your IDE](#run-and-debug-flink-programs-in-your-ide)
1. [Exercises, tests, and solutions](#exercises-tests-and-solutions)

[**Lab exercises**](#lab-exercises)

[**Contributing**](#contributing)

[**License**](#license)

## Set up your development environment

You will need to set up your environment in order to develop, debug, and execute solutions to the training exercises and examples.

### Software requirements

Flink supports Linux, OS X, and Windows as development environments for Flink programs and local execution. The following software is required for a Flink development setup and should be installed on your system:

- Git
- a JDK for Java 8 or Java 11 (a JRE is not sufficient; other versions of Java are currently not supported)
- an IDE for Java (and/or Scala) development with Gradle support
  - We recommend [IntelliJ](https://www.jetbrains.com/idea/), but [Eclipse](https://www.eclipse.org/downloads/) or [Visual Studio Code](https://code.visualstudio.com/) (with the [Java extension pack](https://code.visualstudio.com/docs/java/java-tutorial)) can also be used so long as you stick to Java
  - For Scala, you will need to use IntelliJ (and its [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala/))

> **:information_source: Note for Windows users:** The shell command examples provided in the training instructions are for UNIX systems.
> You may find it worthwhile to setup cygwin or WSL. For developing Flink jobs, Windows works reasonably well: you can run a Flink cluster on a single machine, submit jobs, run the webUI, and execute jobs in the IDE.

### Clone and build the flink-training project

This `flink-training` repository contains exercises, tests, and reference solutions for the programming exercises.

> **:information_source: Repository Layout:** This repository has several branches set up pointing to different Apache Flink versions, similarly to the [apache/flink](https://github.com/apache/flink) repository with:
> - a release branch for each minor version of Apache Flink, e.g. `release-1.10`, and
> - a `master` branch that points to the current Flink release (not `flink:master`!)
>
> If you want to work on a version other than the current Flink release, make sure to check out the appropriate branch.

Clone the `flink-training` repository from GitHub, navigate into the project repository, and build it:

```bash
git clone https://github.com/apache/flink-training.git
cd flink-training
./gradlew test shadowJar
```

If this is your first time building it, you will end up downloading all of the dependencies for this Flink training project. This usually takes a few minutes, depending on the speed of your internet connection.

If all of the tests pass and the build is successful, you are off to a good start.

<details>
<summary><strong>:cn: Users in China: click here for instructions on using a local Maven mirror.</strong></summary>

If you are in China, we recommend configuring the Maven repository to use a mirror. You can do this by uncommenting this section in our [`build.gradle`](build.gradle) file:

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
<summary><strong>Enable Scala (optional)</strong></summary>

The exercises in this project are also available in Scala but due to a couple
of reported problems from non-Scala users, we decided to disable these by
default. You can re-enable all Scala exercises and solutions adapting the
[`gradle.properties`](gradle.properties) file like this:

```properties
#...

# Scala exercises can be enabled by setting this to true
org.gradle.project.enable_scala = true
```

You can also selectively apply this plugin in a single subproject if desired.
</details>


### Import the flink-training project into your IDE

The project needs to be imported as a gradle project into your IDE.

Then you should be able to open [`RideCleansingTest`](ride-cleansing/src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingTest.java) and run this test.

> **:information_source: Note for Scala users:** You will need to use IntelliJ with the JetBrains Scala plugin, and you will need to add a Scala 2.12 SDK to the Global Libraries section of the Project Structure as well as to the module you are working on.
> IntelliJ will ask you for the latter when you open a Scala file.
> Please note that Scala 2.12.8 and above are not supported (see [Flink Scala Versions](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/project-configuration/#scala-versions for details))!

## Use the taxi data streams

These exercises use data [generators](common/src/main/java/org/apache/flink/training/exercises/common/sources) that produce simulated event streams.
The data is inspired by the [New York City Taxi & Limousine Commission's](http://www.nyc.gov/html/tlc/html/home/home.shtml) public
[data set](https://uofi.app.box.com/NYCtaxidata) about taxi rides in New York City.

### Schema of taxi ride events

Our taxi data set contains information about individual taxi rides in New York City.

Each ride is represented by two events: a trip start, and a trip end.

Each event consists of eleven fields:

```
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
eventTime      : Instant   // the timestamp for this event
startLon       : Float     // the longitude of the ride start location
startLat       : Float     // the latitude of the ride start location
endLon         : Float     // the longitude of the ride end location
endLat         : Float     // the latitude of the ride end location
passengerCnt   : Short     // number of passengers on the ride
```

### Schema of taxi fare events

There is also a related data set containing fare data about those same rides, with the following fields:

```
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
startTime      : Instant   // the start time for this ride
paymentType    : String    // CASH or CARD
tip            : Float     // tip for this ride
tolls          : Float     // tolls for this ride
totalFare      : Float     // total fare collected
```

## How to do the lab exercises

In the hands-on sessions, you will implement Flink programs using various Flink APIs.

The following steps guide you through the process of using the provided data streams, implementing your first Flink streaming program, and executing your program in your IDE.

We assume you have set up your development environment according to our [setup guide](#set-up-your-development-environment).

### Learn about the data

The initial set of exercises are all based on data streams of events about taxi rides and taxi fares. These streams are produced by source functions which reads data from input files.
Read the [instructions](#using-the-taxi-data-streams) to learn how to use them.

### Run and debug Flink programs in your IDE

Flink programs can be executed and debugged from within an IDE. This significantly eases the development process and provides an experience similar to working on any other Java (or Scala) application.

To start a Flink program in your IDE, run its `main()` method. Under the hood, the execution environment will start a local Flink instance within the same process. Hence, it is also possible to put breakpoints in your code and debug it.

If you have an IDE with this `flink-training` project imported, you can run (or debug) a streaming job by:

- opening the `org.apache.flink.training.examples.ridecount.RideCountExample` class in your IDE
- running (or debugging) the `main()` method of the `RideCountExample` class using your IDE

### Exercises, tests, and solutions

Each of these exercises include:
- an `...Exercise` class with most of the necessary boilerplate code for getting started
- a JUnit Test class (`...Test`) with a few tests for your implementation
- a `...Solution` class with a complete solution

There are Java and Scala versions of all the exercise, test, and solution classes. They can each be run from IntelliJ.

> **:information_source: Note:** As long as your `...Exercise` class is throwing a `MissingSolutionException`, the provided JUnit test classes will ignore that failure and verify the correctness of the solution implementation instead.

You can run exercises, solutions, and tests with the `gradlew` command.

To run tests:

```bash
./gradlew test
./gradlew :<subproject>:test
```

For Java/Scala exercises and solutions, we provide special tasks that can be listed with:

```bash
./gradlew printRunTasks
```

:point_down: Now you are ready to begin the lab exercises. :point_down:

## Lab exercises

1. [Filtering a Stream (Ride Cleansing)](ride-cleansing)
1. [Stateful Enrichment (Rides and Fares)](rides-and-fares)
1. [Windowed Analytics (Hourly Tips)](hourly-tips)
   - [Exercise](hourly-tips/README.md)
   - [Discussion](hourly-tips/DISCUSSION.md)
1. [`ProcessFunction` and Timers (Long Ride Alerts)](long-ride-alerts)
   - [Exercise](long-ride-alerts/README.md)
   - [Discussion](long-ride-alerts/DISCUSSION.md)

## Contribute

If you would like to contribute to this repository or add new exercises, please read the [contributing](CONTRIBUTING.md) guide.

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
