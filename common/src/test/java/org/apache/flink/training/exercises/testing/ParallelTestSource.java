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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

public class ParallelTestSource<T> extends RichParallelSourceFunction<T> {
    protected T[] testStream;
    TestSourcePartitioner<T> partitioner;
    private List<T> substream;

    public ParallelTestSource(TestSourcePartitioner<T> partitioner, T... events) {
        this.partitioner = partitioner;
        this.testStream = events;
    }

    public ParallelTestSource(T... events) {
        this.partitioner = (e -> 0);
        this.testStream = events;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        substream = new ArrayList<>();

        for (T element : testStream) {
            long subtaskToUse = partitioner.partition(element) % numberOfParallelSubtasks;

            if (subtaskToUse == indexOfThisSubtask) {
                substream.add(element);
            } else if (subtaskToUse < 0 || subtaskToUse > numberOfParallelSubtasks - 1) {
                throw new RuntimeException("Requested subtask is out-of-bounds: " + subtaskToUse);
            }
        }
    }

    @Override
    public void run(SourceContext<T> ctx) {
        for (T element : substream) {
            ctx.collect(element);
        }
        // test sources are finite, so they emit a Long.MAX_VALUE watermark when they finish
    }

    @Override
    public void cancel() {
        // ignore cancel, finite anyway
    }
}
