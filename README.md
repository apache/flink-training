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

# Apache Flink Training Exercises

Exercises that go along with the training content in the documentation.

## Table of Contents

[**Set up your Development Environment**](#set-up-your-development-environment)

1. [Software requirements](#software-requirements)
1. [Clone and build the flink-training project](#clone-and-build-the-flink-training-project)
1. [Import the flink-training project into your IDE](#import-the-flink-training-project-into-your-ide)

[**Using the Taxi Data Streams**](#using-the-taxi-data-streams)

1. [Schema of Taxi Ride Events](#schema-of-taxi-ride-events)
1. [Generating Taxi Ride Data Streams in a Flink program](#generating-taxi-ride-data-streams-in-a-flink-program)

[**How to do the Labs**](#how-to-do-the-labs)

1. [Learn about the data](#learn-about-the-data)
1. [Modify `ExerciseBase`](#modify-exercisebase)
1. [Run and debug Flink programs in your IDE](#run-and-debug-flink-programs-in-your-ide)
1. [Exercises, Tests, and Solutions](#exercises-tests-and-solutions)

[**Labs**](LABS-OVERVIEW.md)

[**License**](#license)

## Set up your Development Environment

The following instructions guide you through the process of setting up a development environment for the purpose of developing, debugging, and executing solutions to the Flink developer training exercises and examples.

### Software requirements

Flink supports Linux, OS X, and Windows as development environments for Flink programs and local execution. The following software is required for a Flink development setup and should be installed on your system:

- a JDK for Java 8 or Java 11 (a JRE is not sufficient; other versions of Java are currently not supported)
- Git
- an IDE for Java (and/or Scala) development with Gradle support.
  We recommend [IntelliJ](https://www.jetbrains.com/idea/), but [Eclipse](https://www.eclipse.org/downloads/) or [Visual Studio Code](https://code.visualstudio.com/) (with the [Java extension pack](https://code.visualstudio.com/docs/java/java-tutorial)) can also be used so long as you stick to Java. For Scala, you will need to use IntelliJ (and its [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala/)).

> **:information_source: Note for Windows users:** The examples of shell commands provided in the training instructions are for UNIX systems. To make things easier, you may find it worthwhile to setup cygwin or WSL. For developing Flink jobs, Windows works reasonably well: you can run a Flink cluster on a single machine, submit jobs, run the webUI, and execute jobs in the IDE.

### Clone and build the flink-training project

This `flink-training` project contains exercises, tests, and reference solutions for the programming exercises. Clone the `flink-training` project from Github and build it.

> **:information_source: Repository Layout:** This repository has several branches set up pointing to different Apache Flink versions, similarly to the [apache/flink](https://github.com/apache/flink) repository with:
> - a release branch for each minor version of Apache Flink, e.g. `release-1.10`, and
> - a `master` branch that points to the current Flink release (not `flink:master`!)
>
> If you want to work on a version other than the current Flink release, make sure to check out the appropriate branch.

```bash
git clone https://github.com/apache/flink-training.git
cd flink-training
./gradlew test shadowJar
```

If you haven’t done this before, at this point you’ll end up downloading all of the dependencies for this Flink training project. This usually takes a few minutes, depending on the speed of your internet connection.

If all of the tests pass and the build is successful, you are off to a good start.

<details>
<summary><strong>Users in China: click here for instructions about using a local maven mirror.</strong></summary>

If you are in China, we recommend configuring the maven repository to use a mirror. You can do this by uncommenting the appropriate line in our [`build.gradle`](build.gradle) like this:

```groovy
    repositories {
        // for access from China, you may need to uncomment this line
        maven { url 'http://maven.aliyun.com/nexus/content/groups/public/' }
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


### Enable Scala (optional)

The exercises in this project are also available in Scala but due to a couple
of reported problems from non-Scala users, we decided to disable these by
default. You can re-enable all Scala exercises and solutions by uncommenting
the Scala plugin in our [`build.gradle`](build.gradle) file:

```groovy
subprojects {
    //...
    apply plugin: 'scala' // optional; uncomment if needed
}
```

You can also selectively apply this plugin in a single subproject if desired.

### Import the flink-training project into your IDE

The project needs to be imported as a gradle project into your IDE.

Once that’s done you should be able to open [`RideCleansingTest`](ride-cleansing/src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingTest.java) and successfully run this test.

> **:information_source: Note for Scala users:** You will need to use IntelliJ with the JetBrains Scala plugin, and you will need to add a Scala 2.12 SDK to the Global Libraries section of the Project Structure. IntelliJ will ask you for the latter when you open a Scala file.

## Using the Taxi Data Streams

These exercises use data [generators](common/src/main/java/org/apache/flink/training/exercises/common/sources) that produce simulated event streams
inspired by those shared by the [New York City Taxi & Limousine Commission](http://www.nyc.gov/html/tlc/html/home/home.shtml)
in their public [data set](https://uofi.app.box.com/NYCtaxidata) about taxi rides in New York City.

### Schemas of Taxi Ride and Taxi Fare Events

Our taxi data set contains information about individual taxi rides in New York City. Each ride is represented by two events: a trip start, and a trip end event. Each event consists of eleven fields:

```
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
startTime      : Instant   // the start time of a ride
endTime        : Instant   // the end time of a ride,
                           //   "1970-01-01 00:00:00" for start events
startLon       : Float     // the longitude of the ride start location
startLat       : Float     // the latitude of the ride start location
endLon         : Float     // the longitude of the ride end location
endLat         : Float     // the latitude of the ride end location
passengerCnt   : Short     // number of passengers on the ride
```

There is also a related data set containing fare data about those same rides, with these fields:

```
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
startTime      : Instant   // the start time of a ride
paymentType    : String    // CASH or CARD
tip            : Float     // tip for this ride
tolls          : Float     // tolls for this ride
totalFare      : Float     // total fare collected
```

## How to do the Labs

In the hands-on sessions you will implement Flink programs using various Flink APIs.

The following steps guide you through the process of using the provided data streams, implementing your first Flink streaming program, and executing your program in your IDE.

We assume you have set up your development environment according to our [setup guide above](#set-up-your-development-environment).

### Learn about the data

The initial set of exercises are all based on data streams of events about taxi rides and taxi fares. These streams are produced by source functions which reads data from input files. Please read the [instructions above](#using-the-taxi-data-streams) to learn how to use them.

### Run and debug Flink programs in your IDE

Flink programs can be executed and debugged from within an IDE. This significantly eases the development process and provides an experience similar to working on any other Java (or Scala) application.

Starting a Flink program in your IDE is as easy as running its `main()` method. Under the hood, the execution environment will start a local Flink instance within the same process. Hence it is also possible to put breakpoints in your code and debug it.

Assuming you have an IDE with this `flink-training` project imported, you can run (or debug) a simple streaming job as follows:

- Open the `org.apache.flink.training.examples.ridecount.RideCountExample` class in your IDE
- Run (or debug) the `main()` method of the `RideCountExample` class using your IDE.

### Exercises, Tests, and Solutions

Each of these exercises includes an `...Exercise` class with most of the necessary boilerplate code for getting started, as well as a JUnit Test class (`...Test`) with a few tests for your implementation, and a `...Solution` class with a complete solution.

> **:information_source: Note:** As long as your `...Exercise` class is throwing a `MissingSolutionException`, the provided JUnit test classes will ignore that failure and verify the correctness of the solution implementation instead.

There are Java and Scala versions of all the exercise, test, and solution classes, each of which can be run from IntelliJ as usual.

#### Running Exercises, Tests, and Solutions on the Command Line

You can execute exercises, solutions, and tests via `gradlew` from a CLI.

- Tests can be executed as usual:

    ```bash
    ./gradlew test
    ./gradlew :<subproject>:test
    ```

- For Java/Scala exercises and solutions, we provide special tasks that are listed via

    ```bash
    ./gradlew printRunTasks
    ```

-----

Now you are ready to begin with the first exercise in our [**Labs**](LABS-OVERVIEW.md).

-----

## How to work on this project

> **:heavy_exclamation_mark: Important:** This section contains tips for developers who are
> maintaining the `flink-training` project (not so much people doing the
> training).

The following sections apply on top of the [Setup Instructions](#set-up-your-development-environment) above.

### Code Formatting

Just like [Apache Flink](https://github.com/apache/flink), we use the [Spotless
plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) together
with [google-java-format](https://github.com/google/google-java-format) to
format our Java code. You can configure your IDE to automatically apply
formatting on saving with these steps:

1. Install the [google-java-format
   plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format) and
   enable it for the project
2. In the plugin settings, change the code style to "AOSP" (4-space indents)
3. Install the [Save Actions
   plugin](https://plugins.jetbrains.com/plugin/7642-save-actions)
4. Enable the plugin, along with "Optimize imports" and "Reformat file" but
   ignoring `.*README\.md`.

### Ignoring Refactoring Commits

We keep a list of big refactoring commits in `.git-blame-ignore-revs`. When looking at change annotations using `git blame` it's helpful to ignore these. You can configure git and your IDE to do so using:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

### Adding new exercises

If you want to add a new exercise, we recommend copying an existing one and
adapting it accordingly. Make sure the new subproject's `build.gradle` file
contains appropriate class name properties so that we can create the right
tasks for [Running Tests and Solutions on the Command Line](#running-exercises-tests-and-solutions-on-the-command-line), e.g.:

```groovy
ext.javaExerciseClassName = 'org.apache.flink.training.exercises.ridesandfares.RidesAndFaresExercise'
ext.scalaExerciseClassName = 'org.apache.flink.training.exercises.ridesandfares.scala.RidesAndFaresExercise'
ext.javaSolutionClassName = 'org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution'
ext.scalaSolutionClassName = 'org.apache.flink.training.solutions.ridesandfares.scala.RidesAndFaresSolution'

apply plugin: 'application'

mainClassName = ext.javaExerciseClassName
```

### Useful Gradle Commands and Tricks

#### Clean Build with all Checks

```bash
./gradlew clean check shadowJar
./gradlew clean check shadowJar --no-build-cache
```

#### Force a Re-run of all Tests Only

```bash
./gradlew cleanTest test  --no-build-cache
```

> **:information_source: Note:** Ignoring the build-cache is required if you really want to run the test again (without any changes in code).
> Otherwise the test tasks will just pull the latest test results from the cache.

#### Fix Formatting

```bash
./gradlew spotlessApply
./gradlew :rides-and-fares:spotlessApply
```

> **:information_source: Note:** You actually do not have to remember this since
> `./gradlew check` will not only verify the formatting and print any errors but
> also mention the command to fix these.

#### Tune the Java Compiler

Add the following code to the `subprojects { /*...*/ }` section of the
[`build.gradle`](build.gradle) file and adapt accordingly, for example:

```groovy
    tasks.withType(JavaCompile) {
        options.compilerArgs << '-Xlint:unchecked'
        options.deprecation = true
    }
```

> **:information_source: Note:** We don't add this by default for now to keep
> the training requirements low for participants and keep focus on the
> exercises at hand.

#### Deprecated Gradle Features

You may see this warning being reported from Gradle:
> Deprecated Gradle features were used in this build, making it incompatible with Gradle 8.0.
>
> You can use '--warning-mode all' to show the individual deprecation warnings and determine if they come from your own scripts or plugins.
>
> See https://docs.gradle.org/7.1/userguide/command_line_interface.html#sec:command_line_warnings

This is currently caused by https://github.com/johnrengelman/shadow/issues/680
and has to be fixed by the shadow plugin developers.

-----

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
