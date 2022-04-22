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

# 练习讨论: 窗口分析（每小时小费）

(关于[窗口分析（每小时小费）](./README_zh.md)的讨论)

尽管有很多相似之处，Java 和 Scala 参考解决方案展示了两种不同的方法.
两者首先都计算每个司机每小时的小费总和。
[`HourlyTipsSolution.java`](src/solution/java/org/apache/flink/training/solutions/hourlytips/HourlyTipsSolution.java) 看起来像这样，

```java
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
    .keyBy((TaxiFare fare) -> fare.driverId)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .process(new AddTips());
```

其中， `ProcessWindowFunction` 完成了所有繁重的工作：

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

这很简单，但缺点是它会缓冲窗口中所有的 `TaxiFare` 对象，直到窗口被触发。
相比使用 `reduce` 或 `aggregate` 方法来增量计算小费总额，这种方法的效率较低。

[Scala 解决方案](src/solution/scala/org/apache/flink/training/solutions/hourlytips/scala/HourlyTipsSolution.scala)使用了 `reduce` 函数：

```scala
val hourlyTips = fares
  .map((f: TaxiFare) => (f.driverId, f.tip))
  .keyBy(_._1)
  .window(TumblingEventTimeWindows.of(Time.hours(1)))
  .reduce(
    (f1: (Long, Float), f2: (Long, Float)) => { (f1._1, f1._2 + f2._2) },
    new WrapWithWindowInfo())
```

连同这样的 `ProcessWindowFunction`：

```scala
class WrapWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
    val sumOfTips = elements.iterator.next()._2
    out.collect((context.window.getEnd(), key, sumOfTips))
  }
}
```

以计算 `hourlyTips`.

计算出 `hourlyTips` 之后，让我们来看看这个流是什么样的。`hourlyTips.print()` 产生了类似这样的结果：

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

可以看到，每个小时都有大量的三元组显示每个司机在这一个小时内的小费总额。

现在，如何找到每个小时内的最大值？ 参考解决方案或多或少都这样做：

```java
DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
    .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
    .maxBy(2);
```

这样做也不错，因为它产生了正确的结果流：

```
3> (1577883600000,2013000089,76.0)
4> (1577887200000,2013000197,71.0)
1> (1577890800000,2013000118,83.0)
2> (1577894400000,2013000119,81.0)
3> (1577898000000,2013000195,73.0)
4> (1577901600000,2013000072,123.0)
```

但是，如果换成这样呢？

```java
DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
    .keyBy(t -> t.f0)
    .maxBy(2);
```

这表示按时间戳对 `hourlyTips` 流进行分组，并在每个时间戳分组内找到小费总和的最大值，而听起来这正是我们想要的。
虽然这个替代方案确实找到了相同的结果，但是有几个原因可以解释它为什么不是一个很好的解决方案。

首先，这种方法不是在每个窗口的结束时产生一个结果，而是创建了一个连续报告每个键值（即每小时）迄今为止达到的最大值的流。
如果仅仅是想得到每个小时中的一个单一值的话，那么这是一种笨拙的消费方式。

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

其次，Flink 将永远保持每个键值（每小时）迄今为止出现的最大值。
Flink 不知道这些键值是事件的时间戳，也不知道水位线可以被用作何时清除此状态的指示器——为了获得这些语义，我们需要使用窗口。

-----

[**返回练习概述**](../README_zh.md#lab-exercises)
