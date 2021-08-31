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

package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class ParallelTestSource<T> extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T> {
    private final T[] testStream;
    private final TypeInformation<T> typeInfo;

    @SuppressWarnings("unchecked")
    @SafeVarargs
    public ParallelTestSource(T... events) {
        this.typeInfo = (TypeInformation<T>) TypeExtractor.createTypeInfo(events[0].getClass());
        this.testStream = events;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int subtask = 0;

        // the elements of the testStream are assigned to the parallel instances in a round-robin
        // fashion
        for (T element : testStream) {
            if (subtask == indexOfThisSubtask) {
                ctx.collect(element);
            }
            subtask = (subtask + 1) % numberOfParallelSubtasks;
        }

        // test sources are finite, so they emit a Long.MAX_VALUE watermark when they finish
    }

    @Override
    public void cancel() {
        // ignore cancel, finite anyway
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInfo;
    }
}
