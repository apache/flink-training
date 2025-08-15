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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestSink<OUT> implements Sink<OUT> {

    private final String name;

    private final String valueUuId;

    public static final ConcurrentHashMap<String, ConcurrentLinkedQueue<Object>> RESULTS =
            new ConcurrentHashMap<>();

    public Collection<OUT> getResults() {
        ConcurrentLinkedQueue<OUT> result = (ConcurrentLinkedQueue<OUT>) RESULTS.get(valueUuId);
        if (result == null) {
            return List.of();
        }
        return result;
    }

    public TestSink(String name) {
        this.name = name;
        valueUuId = UUID.randomUUID().toString();
    }

    public TestSink() {
        this("results");
    }

    @Override
    public SinkWriter<OUT> createWriter(WriterInitContext context) throws IOException {
        return new TestSinkWriter<>(valueUuId);
    }

    public static class TestSinkWriter<OUT> implements SinkWriter<OUT> {

        private final String valueUuId;

        public TestSinkWriter(String valueUuId) {
            this.valueUuId = valueUuId;
        }

        @Override
        public void write(OUT element, Context context) throws IOException, InterruptedException {
            RESULTS.computeIfAbsent(valueUuId, k -> new ConcurrentLinkedQueue<>()).add(element);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            System.out.println("TestSinkWriter.flush");
        }

        @Override
        public void close() throws Exception {
            System.out.println("TestSinkWriter.close");
        }
    }
}
