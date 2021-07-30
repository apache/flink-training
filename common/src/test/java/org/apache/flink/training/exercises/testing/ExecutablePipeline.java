package org.apache.flink.training.exercises.testing;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface ExecutablePipeline<IN, OUT> {
    void execute(SourceFunction<IN> source, TestSink<OUT> sink) throws Exception;
}
