package org.apache.flink.training.exercises.testing;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * A KeyedProcessFunction that can delegate to either a KeyedProcessFunction in the exercise or in
 * the solution. The implementation in the exercise is tested first, and if it throws
 * MissingSolutionException, then the solution is tested instead.
 *
 * <p>This can be used to write test harness tests.
 *
 * @param <K> key type
 * @param <IN> input type
 * @param <OUT> output type
 */
public class ComposedKeyedProcessFunction<K, IN, OUT> extends KeyedProcessFunction<K, IN, OUT> {
    private KeyedProcessFunction<K, IN, OUT> exercise;
    private KeyedProcessFunction<K, IN, OUT> solution;
    private boolean useExercise;

    public ComposedKeyedProcessFunction(
            KeyedProcessFunction<K, IN, OUT> exercise, KeyedProcessFunction<K, IN, OUT> solution) {

        this.exercise = exercise;
        this.solution = solution;
        this.useExercise = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        try {
            exercise.setRuntimeContext(this.getRuntimeContext());
            exercise.open(parameters);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                this.useExercise = false;
                solution.setRuntimeContext(this.getRuntimeContext());
                solution.open(parameters);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void processElement(
            IN value,
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<K, IN, OUT>.Context ctx,
            Collector<OUT> out)
            throws Exception {

        if (useExercise) {
            exercise.processElement(value, ctx, out);
        } else {
            solution.processElement(value, ctx, out);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<K, IN, OUT>.OnTimerContext
                    ctx,
            Collector<OUT> out)
            throws Exception {

        if (useExercise) {
            exercise.onTimer(timestamp, ctx, out);
        } else {
            solution.onTimer(timestamp, ctx, out);
        }
    }
}
