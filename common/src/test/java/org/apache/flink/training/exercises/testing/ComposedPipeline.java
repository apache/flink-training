package org.apache.flink.training.exercises.testing;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

/**
 * This allows the tests to be run against both the exercises and the solutions.
 *
 * <p>If an exercise throws MissingSolutionException, then the solution is tested.
 */
public class ComposedPipeline<IN, OUT> implements ExecutablePipeline<IN, OUT> {

    private ExecutablePipeline<IN, OUT> exercise;
    private ExecutablePipeline<IN, OUT> solution;

    public ComposedPipeline(
            ExecutablePipeline<IN, OUT> exercise, ExecutablePipeline<IN, OUT> solution) {
        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public void execute(SourceFunction<IN> source, TestSink<OUT> sink) throws Exception {

        sink.reset();

        try {
            exercise.execute(source, sink);
        } catch (Exception e) {
            if (ultimateCauseIsMissingSolution(e)) {
                solution.execute(source, sink);
            } else {
                throw e;
            }
        }
    }

    private boolean ultimateCauseIsMissingSolution(Throwable e) {
        if (e instanceof MissingSolutionException) {
            return true;
        } else if (e.getCause() != null) {
            return ultimateCauseIsMissingSolution(e.getCause());
        } else {
            return false;
        }
    }
}
