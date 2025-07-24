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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.lineage.DefaultLineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelTestSource<T>
        implements Source<T, ParallelTestSource.InMemorySplit<T>, List<T>>,
                ResultTypeQueryable<T>,
                OutputTypeConfigurable<T>,
                LineageVertexProvider {

    /** The (de)serializer to be used for the data elements. */
    private TypeSerializer<T> serializer;

    private final List<T> elements;

    private final TypeInformation<T> typeInfo;

    @SafeVarargs
    public ParallelTestSource(T... elements) {
        this(List.of(elements));
    }

    public ParallelTestSource(Collection<T> elements) {
        this(null, null, elements);
    }

    @SafeVarargs
    public ParallelTestSource(
            @Nullable TypeSerializer<T> serializer,
            @Nullable TypeInformation<T> typeInfo,
            T... elements) {
        this(serializer, typeInfo, List.of(elements));
    }

    public ParallelTestSource(
            @Nullable TypeSerializer<T> serializer,
            @Nullable TypeInformation<T> typeInfo,
            @Nonnull Collection<T> elements) {
        this.elements = new ArrayList<>(elements);

        if (typeInfo != null) {
            this.typeInfo = typeInfo;
        } else {
            if (this.elements.isEmpty()) {
                throw new IllegalArgumentException(
                        "The type information must be specified when the collection is empty");
            }
            T firstElement = this.elements.get(0);
            try {
                this.typeInfo = TypeExtractor.getForObject(firstElement);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Could not create TypeInformation for type "
                                + firstElement.getClass().getName()
                                + "; please specify the TypeInformation manually",
                        e);
            }
        }

        checkIterable(elements, this.typeInfo.getTypeClass());

        this.serializer =
                serializer != null
                        ? serializer
                        : new KryoSerializer<T>(
                                this.typeInfo.getTypeClass(), new SerializerConfigImpl());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, InMemorySplit<T>> createReader(SourceReaderContext ctx) {
        return new InMemoryReader<>();
    }

    @Override
    public SplitEnumerator<InMemorySplit<T>, List<T>> createEnumerator(
            SplitEnumeratorContext<InMemorySplit<T>> enumContext) {
        return new InMemoryEnumerator<>(enumContext, elements);
    }

    @Override
    public SplitEnumerator<InMemorySplit<T>, List<T>> restoreEnumerator(
            SplitEnumeratorContext<InMemorySplit<T>> enumContext, List<T> checkpoint) {
        return new InMemoryEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<InMemorySplit<T>> getSplitSerializer() {
        return new InMemorySplitSerializer<>(this.serializer);
    }

    @Override
    public SimpleVersionedSerializer<List<T>> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer<>(this.serializer);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.typeInfo;
    }

    /** Split definition for in-memory data. */
    public static class InMemorySplit<T> implements SourceSplit, Serializable {
        private final int splitId;
        private final List<T> slice;

        InMemorySplit(int splitId, List<T> slice) {
            this.splitId = splitId;
            this.slice = new ArrayList<>(slice);
        }

        @Override
        public String splitId() {
            return "split-" + splitId;
        }

        public List<T> getSlice() {
            return slice;
        }
    }

    /** SplitEnumerator：split data. */
    public static class InMemoryEnumerator<T>
            implements SplitEnumerator<InMemorySplit<T>, List<T>> {

        private final SplitEnumeratorContext<InMemorySplit<T>> context;
        private final List<T> elements;
        private boolean assigned = false;

        InMemoryEnumerator(SplitEnumeratorContext<InMemorySplit<T>> context, List<T> elements) {
            this.context = context;
            this.elements = elements;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addSplitsBack(List<InMemorySplit<T>> splits, int subtaskId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addReader(int subtaskId) {
            if (!assigned && context.registeredReaders().size() == context.currentParallelism()) {
                assignSplits();
                assigned = true;
            }
        }

        private void assignSplits() {
            int parallelism = context.currentParallelism();
            int step = Math.max(1, (elements.size() + parallelism - 1) / parallelism);
            for (int i = 0; i < parallelism; i++) {
                int from = i * step;
                int to = Math.min(from + step, elements.size());
                if (from >= to) {
                    while (i < parallelism) {
                        InMemorySplit<T> split = new InMemorySplit<>(i, List.of());
                        context.assignSplit(split, i);
                        i++;
                    }
                    break;
                }
                InMemorySplit<T> split = new InMemorySplit<>(i, elements.subList(from, to));
                context.assignSplit(split, i);
            }
        }

        @Override
        public List<T> snapshotState(long checkpointId) {
            return elements;
        }

        @Override
        public void close() {}
    }

    /** SourceReader: read data. */
    public static class InMemoryReader<T> implements SourceReader<T, InMemorySplit<T>> {

        private final Queue<T> remaining = new ArrayDeque<>();
        private final AtomicBoolean initialized = new AtomicBoolean(false);

        public InMemoryReader() {}

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<T> output) {
            if (!initialized.get()) {
                return InputStatus.MORE_AVAILABLE;
            }
            T next = remaining.poll();
            if (next != null) {
                output.collect(next);
                return InputStatus.MORE_AVAILABLE;
            } else {
                return InputStatus.END_OF_INPUT;
            }
        }

        @Override
        public List<InMemorySplit<T>> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture((Void) null);
        }

        @Override
        public void addSplits(List<InMemorySplit<T>> splits) {
            for (InMemorySplit<T> split : splits) {
                remaining.addAll(split.getSlice());
            }
            initialized.set(true);
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}
    }

    public static class InMemorySplitSerializer<T>
            implements SimpleVersionedSerializer<InMemorySplit<T>> {

        /** The (de)serializer to be used for the data elements. */
        private final TypeSerializer<T> serializer;

        public InMemorySplitSerializer(TypeSerializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(InMemorySplit<T> split) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
            try {
                wrapper.writeInt(split.splitId);
                wrapper.writeInt(split.slice.size());
                for (T element : split.slice) {
                    serializer.serialize(element, wrapper);
                }
            } catch (Exception e) {
                throw new IOException(
                        "Serializing the source elements failed: " + e.getMessage(), e);
            }
            return baos.toByteArray();
        }

        @Override
        public InMemorySplit<T> deserialize(int version, byte[] serialized) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
            DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
            try {
                int splitId = wrapper.readInt();
                int size = wrapper.readInt();
                List<T> result = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    result.add(serializer.deserialize(wrapper));
                }
                return new InMemorySplit<>(splitId, result);
            } catch (IOException e) {
                throw new IOException(
                        "Deserializing the source elements failed: " + e.getMessage(), e);
            }
        }
    }

    public static class CheckpointSerializer<T> implements SimpleVersionedSerializer<List<T>> {

        /** The (de)serializer to be used for the data elements. */
        private final TypeSerializer<T> serializer;

        public CheckpointSerializer(TypeSerializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(List<T> obj) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
            try {
                wrapper.writeInt(obj.size());
                for (T element : obj) {
                    serializer.serialize(element, wrapper);
                }
            } catch (Exception e) {
                throw new IOException(
                        "Serializing the source elements failed: " + e.getMessage(), e);
            }
            return baos.toByteArray();
        }

        @Override
        public List<T> deserialize(int version, byte[] serialized) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
            DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
            try {
                int size = wrapper.readInt();
                List<T> result = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    result.add(serializer.deserialize(wrapper));
                }
                return result;
            } catch (IOException e) {
                throw new IOException(
                        "Deserializing the source elements failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Set element type and re-serialize element if required. Should only be called before
     * serialization/deserialization of this function.
     */
    @Override
    public void setOutputType(TypeInformation<T> outTypeInfo, ExecutionConfig executionConfig) {
        Preconditions.checkState(
                elements != null,
                "The output type should've been specified before shipping the graph to the cluster");
        checkIterable(elements, outTypeInfo.getTypeClass());
        TypeSerializer<T> newSerializer =
                outTypeInfo.createSerializer(executionConfig.getSerializerConfig());
        if (Objects.equals(serializer, newSerializer)) {
            return;
        }
        serializer = newSerializer;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Verifies that all elements in the collection are non-null, and are of the given class, or a
     * subclass thereof.
     *
     * @param elements The collection to check.
     * @param viewedAs The class to which the elements must be assignable to.
     * @param <OUT> The generic type of the collection to be checked.
     */
    public static <OUT> void checkCollection(Collection<OUT> elements, Class<OUT> viewedAs) {
        checkIterable(elements, viewedAs);
    }

    private static <OUT> void checkIterable(Iterable<OUT> elements, Class<?> viewedAs) {
        for (OUT elem : elements) {
            if (elem == null) {
                throw new IllegalArgumentException("The collection contains a null element");
            }

            if (!viewedAs.isAssignableFrom(elem.getClass())) {
                throw new IllegalArgumentException(
                        "The elements in the collection are not all subclasses of "
                                + viewedAs.getCanonicalName());
            }
        }
    }

    @Override
    public LineageVertex getLineageVertex() {
        return new SourceLineageVertex() {
            @Override
            public Boundedness boundedness() {
                return Boundedness.BOUNDED;
            }

            @Override
            public List<LineageDataset> datasets() {
                return List.of(
                        new DefaultLineageDataset(
                                "", "values://FromElementsSource", new HashMap<>()));
            }
        };
    }
}
