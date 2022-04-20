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

# How to contribute to this project

> **:heavy_exclamation_mark: Important:** This section contains tips for developers who are
> maintaining the `flink-training` project (not so much people doing the training).

The following sections apply on top of the [Setup Instructions](README.md#set-up-your-development-environment) above.

## Format code

### Java

Just like [Apache Flink](https://github.com/apache/flink), we use the [Spotless
plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) together
with [google-java-format](https://github.com/google/google-java-format) to
format our Java code. You can configure your IDE to automatically apply
formatting upon saving with these steps:

1. Install the [google-java-format
   plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format) and
   enable it for the project
2. In the plugin settings, enable the plugin and change the code style to "AOSP" (4-space indents).
3. Install the [Save Actions
   plugin](https://plugins.jetbrains.com/plugin/7642-save-actions)
4. Enable the plugin, along with "Optimize imports" and "Reformat file".
5. In the "Save Actions" settings page, set up a "File Path Inclusion" for `.*\.java`. Otherwise, you will get
   unintended reformatting in other files you edit.

### Scala

We use the [Spotless
plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) for
formatting Scala code as well and apply a formatting style similar to the
Scalastyle configuration from [Apache Flink](https://github.com/apache/flink).
The code style is verified during `./gradlew check` which will also print
instructions how to fix the style if it does not comply with the defined
format.

## Ignore refactoring commits

There is a list of refactoring commits in `.git-blame-ignore-revs`.
When looking at change annotations using `git blame`, it is helpful to ignore these.
You can configure git and your IDE to do so with:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

## :heavy_plus_sign: Add new exercises :heavy_plus_sign:

To add a new exercise, we recommend copying an existing one and adapting it. Make sure the new subproject's `build.gradle` file
contains appropriate class name properties so that we can create the right tasks for
[running tests and solutions on the command line](README.md#running-exercises-tests-and-solutions-on-the-command-line).

For example:

```groovy
ext.javaExerciseClassName = 'org.apache.flink.training.exercises.ridesandfares.RidesAndFaresExercise'
ext.scalaExerciseClassName = 'org.apache.flink.training.exercises.ridesandfares.scala.RidesAndFaresExercise'
ext.javaSolutionClassName = 'org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution'
ext.scalaSolutionClassName = 'org.apache.flink.training.solutions.ridesandfares.scala.RidesAndFaresSolution'

apply plugin: 'application'

mainClassName = ext.javaExerciseClassName
```

## Useful Gradle Commands and Tricks

### Clean build with all checks

```bash
./gradlew clean check shadowJar
./gradlew clean check shadowJar --no-build-cache
```

### Force a re-run of all tests only

```bash
./gradlew cleanTest test  --no-build-cache
```

> **:information_source: Note:** Ignoring the build-cache is required if you really want to run the test again
> (without any changes in code). Otherwise the test tasks will just pull the latest test results from the cache.

### Fix formatting

```bash
./gradlew spotlessApply
./gradlew :rides-and-fares:spotlessApply
```

> **:information_source: Note:** You do not have to remember this since `./gradlew check` will not only
> verify the formatting and print any errors but also tell you the command to use to fix these.

### Tune the Java compiler

Add the following code to the `subprojects { /*...*/ }` section of the
[`build.gradle`](build.gradle) file and adapt accordingly. For example:

```groovy
    tasks.withType(JavaCompile) {
        options.compilerArgs << '-Xlint:unchecked'
        options.deprecation = true
    }
```

> **:information_source: Note:** We do not add this by default to keep the training
> requirements low for participants and focus on the exercises.

### :warning: Deprecated Gradle Features

You may see this warning being reported from Gradle:
> Deprecated Gradle features were used in this build, making it incompatible with Gradle 8.0.
>
> You can use '--warning-mode all' to show the individual deprecation warnings and determine if they come from your own scripts or plugins.
>
> See https://docs.gradle.org/7.1/userguide/command_line_interface.html#sec:command_line_warnings

This is currently caused by https://github.com/johnrengelman/shadow/issues/680
and has to be fixed by the shadow plugin developers.
