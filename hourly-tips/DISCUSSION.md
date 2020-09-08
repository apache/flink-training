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

# Lab Discussion: Windowed Analytics (Hourly Tips)

(Discussion of [Lab: Windowed Analytics (Hourly Tips)](./))

The Java and Scala reference solutions illustrate two different approaches, though they have a lot of similarities. Both first compute the sum of the tips for every hour for each driver. In [`HourlyTipsSolution.java`](src/solution/java/org/apache/flink/training/solutions/hourlytips/HourlyTipsSolution.java) that looks like this,

```java
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
	.keyBy((TaxiFare fare) -> fare.driverId)
	.window(TumblingEventTimeWindows.of(Time.hours(1)))
	.process(new AddTips());
```

where a `ProcessWindowFunction` does all the heavy lifting:

```java
public static class AddTips extends ProcessWindowFunction<
		TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
	@Override
	public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
		Float sumOfTips = 0F;
		for (TaxiFare f : fares) {
			sumOfTips += f.tip;
		}
		out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
	}
}
```

This is straightforward, but has the drawback that it is buffering all of the `TaxiFare` objects in the windows until the windows are triggered, which is less efficient than computing the sum of the tips incrementally, using a `reduce` or `agggregate` function. 

The [Scala solution](src/solution/scala/org/apache/flink/training/solutions/hourlytips/scala/HourlyTipsSolution.scala) uses a `reduce` function

```scala
val hourlyTips = fares
  .map((f: TaxiFare) => (f.driverId, f.tip))
  .keyBy(_._1)
  .window(TumblingEventTimeWindows.of(Time.hours(1)))
  .reduce(
    (f1: (Long, Float), f2: (Long, Float)) => { (f1._1, f1._2 + f2._2) },
    new WrapWithWindowInfo())
```

along with this `ProcessWindowFunction`

```scala
class WrapWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
    val sumOfTips = elements.iterator.next()._2
    out.collect((context.window.getEnd(), key, sumOfTips))
  }
}
```

to compute `hourlyTips`.

Having computed `hourlyTips`, it is a good idea to take a look at what this stream looks like. `hourlyTips.print()` yields something like this,

```
2> (1577883600000,2013000185,33.0)
4> (1577883600000,2013000108,14.0)
3> (1577883600000,2013000087,14.0)
1> (1577883600000,2013000036,23.0)
4> (1577883600000,2013000072,13.0)
2> (1577883600000,2013000041,28.0)
3> (1577883600000,2013000123,33.0)
4> (1577883600000,2013000188,18.0)
1> (1577883600000,2013000098,23.0)
2> (1577883600000,2013000047,13.0)
...
```

or in other words, lots of tuples for each hour that show for each driver, the sum of their tips for that hour.

Now, how to find the maximum within each hour? The reference solutions both do this, more or less:

```java
DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
	.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
	.maxBy(2);
```

which works just fine, producing this stream of results:

```
3> (1577883600000,2013000089,76.0)
4> (1577887200000,2013000197,71.0)
1> (1577890800000,2013000118,83.0)
2> (1577894400000,2013000119,81.0)
3> (1577898000000,2013000195,73.0)
4> (1577901600000,2013000072,123.0)
```

But, what if we were to do this, instead?

```java
DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
	.keyBy(t -> t.f0)
	.maxBy(2);
```

This says to group the stream of `hourlyTips` by timestamp, and within each timestamp, find the maximum of the sum of the tips.
That sounds like it is exactly what we want. And while this alternative does find the same results,
there are a couple of reasons why it is not a very good solution.

First, instead of producing a single result at the end of each window, with this approach we get a stream that is
continuously reporting the maximum achieved so far, for each key (i.e., each hour), which is an awkward way to consume
the result if all you wanted was a single value for each hour.

```
1> (1577883600000,2013000108,14.0)
1> (1577883600000,2013000108,14.0)
1> (1577883600000,2013000188,18.0)
1> (1577883600000,2013000188,18.0)
1> (1577883600000,2013000188,18.0)
1> (1577883600000,2013000034,36.0)
1> (1577883600000,2013000183,70.0)
1> (1577883600000,2013000183,70.0)
...
1> (1577883600000,2013000152,73.0)
1> (1577883600000,2013000152,73.0)
...
1> (1577883600000,2013000089,76.0)
...
```

Second, Flink will be keeping in state the maximum seen so far for each key (each hour), forever.
Flink has no idea that these keys are event-time timestamps, and that the watermarks could be used as
an indicator of when this state can be cleared -- to get those semantics, we need to use windows.

-----

[**Back to Labs Overview**](../LABS-OVERVIEW.md)
