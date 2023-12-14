/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source)
                //copied
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (fare, t) -> fare.getEventTimeMillis()))
                //copied
                ;

        DataStream<Tuple3<Long, Long, Float>> hourlySummedTips = fares
                .keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new HourlyWindowAdder());

        hourlySummedTips.map(new PrefixEnrichingMapper("Hourly Sum Per Driver"))
            .print();

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlySummedTips
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .maxBy(2);

        hourlyMax.map(new PrefixEnrichingMapper("Maxxed Per Hour"))
                .print();
        hourlyMax.addSink(sink);


        // execute the pipeline and return the result
        System.out.println("execute Hourly Tips");
        return env.execute("Hourly Tips");
    }

    public static class HourlyWindowAdder extends ProcessWindowFunction<
            TaxiFare,                       // input type
            Tuple3<Long, Long, Float>,      // output type (window end time, driverId, tipsTotal)
            Long,                           // key type
            TimeWindow> {                   // window type
        @Override
        public void process(Long key,
                            Context context,
                            Iterable<TaxiFare> taxiFares,
                            Collector<Tuple3<Long, Long, Float>> out) {
            float tipsAccumulator = 0;
            for (TaxiFare taxiFare: taxiFares) {
                tipsAccumulator += taxiFare.tip;
            }
            out.collect(new Tuple3<>(context.window().getEnd(), key, tipsAccumulator));
        }
    }

    private static class PrefixEnrichingMapper implements MapFunction<
            Tuple3<Long, Long, Float>,
            Tuple4<String, Long, Long, Float>> {
        private final String prefix;
        PrefixEnrichingMapper(String prefix) {
            this.prefix = prefix;
        }
        @Override
        public Tuple4<String, Long, Long, Float> map(Tuple3<Long, Long, Float> t3) throws Exception {
            return Tuple4.of(prefix, t3.f0, t3.f1, t3.f2);
        }
    }
}
