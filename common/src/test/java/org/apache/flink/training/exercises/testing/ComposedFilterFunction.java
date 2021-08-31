package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

public class ComposedFilterFunction<T> implements FilterFunction<T> {

    private final FilterFunction<T> exercise;
    private final FilterFunction<T> solution;

    public ComposedFilterFunction(FilterFunction<T> exercise, FilterFunction<T> solution) {
        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public boolean filter(T value) throws Exception {
        boolean result;

        try {
            result = exercise.filter(value);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                result = solution.filter(value);
            } else {
                throw e;
            }
        }

        return result;
    }
}
