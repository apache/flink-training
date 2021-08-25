package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

/**
 * This allows the tests to be run against both the exercises and the solutions.
 *
 * <p>If an exercise throws MissingSolutionException, then the solution is tested.
 */
public class ComposedPipeline<IN, OUT> implements ExecutablePipeline<IN, OUT> {

    private final ExecutablePipeline<IN, OUT> exercise;
    private final ExecutablePipeline<IN, OUT> solution;

    public ComposedPipeline(
            ExecutablePipeline<IN, OUT> exercise, ExecutablePipeline<IN, OUT> solution) {
        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public JobExecutionResult execute(SourceFunction<IN> source, TestSink<OUT> sink)
            throws Exception {

        JobExecutionResult result;

        try {
            result = exercise.execute(source, sink);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                result = solution.execute(source, sink);
            } else {
                throw e;
            }
        }

        return result;
    }
}
