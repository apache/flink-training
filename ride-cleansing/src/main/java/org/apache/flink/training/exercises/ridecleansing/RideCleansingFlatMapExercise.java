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

package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * The Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class RideCleansingFlatMapExercise {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<EnrichedRide> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public RideCleansingFlatMapExercise(SourceFunction<TaxiRide> source, SinkFunction<EnrichedRide> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        RideCleansingFlatMapExercise job =
                new RideCleansingFlatMapExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {
        Properties props = new Properties();
        props.put("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory");
        Configuration conf = ConfigurationUtils.createConfiguration(props);
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStream<TaxiRide> taxiRideDataStream = env.addSource(new TaxiRideGenerator());

        DataStream<EnrichedRide> enrichedRideDataStream = taxiRideDataStream.flatMap(new FlatMapNYCFilterEnrichment());

        enrichedRideDataStream.addSink(sink);


        // run the pipeline and return the result
        return env.execute("Taxi Ride Cleansing Map");
    }


    public static class FlatMapNYCFilterEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> collector) throws Exception {
            FilterFunction<TaxiRide> nycFilter = new RideCleansingExercise.NYCFilter();
            if(nycFilter.filter(taxiRide)){
                collector.collect(new EnrichedRide(taxiRide));
            }
        }
    }
}