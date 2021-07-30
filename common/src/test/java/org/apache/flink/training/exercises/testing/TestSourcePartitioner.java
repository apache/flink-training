package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.functions.Function;

/**
 * Function to implement a partitioner assigning stream elements to source subtasks.
 *
 * @param <T> The type of the element to be partitioned.
 */
public interface TestSourcePartitioner<T> extends Function {
    /**
     * The subtask index of the parallel source instance used for the given element will be the
     * return value of this function modulo the parallelism.
     *
     * @param element The event to be assigned to a subtask.
     * @return The basis for computing the subtask index.
     */
    long partition(T element);
}
