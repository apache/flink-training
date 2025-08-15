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

package org.apache.flink.training.exercises.common.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This SourceFunction generates a data stream of TaxiRide records.
 *
 * <p>The stream is produced out-of-order.
 */
public class TaxiRideGenerator extends DataGeneratorSource<TaxiRide> {

    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;

    /** TaxiRideGenerator. */
    public TaxiRideGenerator() {
        super(
                new GeneratorFunction<Long, TaxiRide>() {

                    private final AtomicLong id = new AtomicLong(0);
                    private final AtomicLong maxStartTime = new AtomicLong(0);
                    private final PriorityQueue<TaxiRide> endEventQ = new PriorityQueue<>(100);
                    private final ConcurrentLinkedDeque<TaxiRide> deque =
                            new ConcurrentLinkedDeque<>();

                    @Override
                    public TaxiRide map(Long value) throws Exception {
                        synchronized (this) {
                            if (deque.isEmpty()) {
                                long idLocal = id.get();
                                // generate a batch of START events
                                List<TaxiRide> startEvents = new ArrayList<TaxiRide>(BATCH_SIZE);
                                for (int i = 1; i <= BATCH_SIZE; i++) {
                                    TaxiRide ride = new TaxiRide(idLocal + i, true);
                                    startEvents.add(ride);
                                    // the start times may be in order, but let's not assume that
                                    maxStartTime.set(
                                            Math.max(
                                                    maxStartTime.get(), ride.getEventTimeMillis()));
                                }
                                // enqueue the corresponding END events
                                for (int i = 1; i <= BATCH_SIZE; i++) {
                                    endEventQ.add(new TaxiRide(idLocal + i, false));
                                }

                                // release the END events coming before the end of this new batch
                                // (this allows a few END events to precede their matching START
                                // event)
                                while (endEventQ.peek().getEventTimeMillis()
                                        <= maxStartTime.get()) {
                                    TaxiRide ride = endEventQ.poll();
                                    deque.push(ride);
                                }

                                // then emit the new START events (out-of-order)
                                java.util.Collections.shuffle(startEvents, new Random(id.get()));
                                startEvents.iterator().forEachRemaining(deque::push);

                                // prepare for the next batch
                                id.set(id.get() + BATCH_SIZE);
                            }
                            return deque.poll();
                        }
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(200),
                TypeInformation.of(TaxiRide.class));
    }
}
