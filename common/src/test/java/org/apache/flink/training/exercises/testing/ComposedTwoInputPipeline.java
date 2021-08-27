package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

/**
 * This allows the tests to be run against both the exercises and the solutions.
 *
 * <p>If an exercise throws MissingSolutionException, then the solution is tested.
 */
public class ComposedTwoInputPipeline<IN1, IN2, OUT>
        implements ExecutableTwoInputPipeline<IN1, IN2, OUT> {

    private final ExecutableTwoInputPipeline<IN1, IN2, OUT> exercise;
    private final ExecutableTwoInputPipeline<IN1, IN2, OUT> solution;

    public ComposedTwoInputPipeline(
            ExecutableTwoInputPipeline<IN1, IN2, OUT> exercise,
            ExecutableTwoInputPipeline<IN1, IN2, OUT> solution) {

        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public JobExecutionResult execute(
            SourceFunction<IN1> source1, SourceFunction<IN2> source2, TestSink<OUT> sink)
            throws Exception {

        JobExecutionResult result;

        try {
            result = exercise.execute(source1, source2, sink);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                result = solution.execute(source1, source2, sink);
            } else {
                throw e;
            }
        }

        return result;
    }
}
