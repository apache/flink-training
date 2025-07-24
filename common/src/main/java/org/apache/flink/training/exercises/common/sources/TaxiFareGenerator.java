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
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This SourceFunction generates a data stream of TaxiFare records.
 *
 * <p>The stream is generated in order.
 */
public class TaxiFareGenerator extends DataGeneratorSource<TaxiFare> {

    private static Instant limitingTimestamp = Instant.MAX;

    /**
     * build taxi fare deque.
     *
     * @return taxiFareDeque
     */
    public static ConcurrentLinkedDeque<TaxiFare> buildTaxiFareDeque() {
        ConcurrentLinkedDeque<TaxiFare> taxiFareDeque = new ConcurrentLinkedDeque<>();
        for (int i = 1; ; i++) {
            TaxiFare fare = new TaxiFare(i);
            // don't emit events that exceed the specified limit
            if (fare.startTime.compareTo(limitingTimestamp) >= 0) {
                break;
            }
            taxiFareDeque.push(fare);
        }
        return taxiFareDeque;
    }

    /** TaxiFareGenerator. */
    public TaxiFareGenerator() {
        this(buildTaxiFareDeque());
    }

    /**
     * TaxiFareGenerator.
     *
     * @param taxiFareDeque taxiFareDeque
     */
    public TaxiFareGenerator(ConcurrentLinkedDeque<TaxiFare> taxiFareDeque) {
        super(
                new GeneratorFunction<Long, TaxiFare>() {

                    private final AtomicLong id = new AtomicLong(0);
                    private final AtomicLong maxStartTime = new AtomicLong(0);

                    @Override
                    public TaxiFare map(Long value) throws Exception {
                        synchronized (this) {
                            return taxiFareDeque.poll();
                        }
                    }
                },
                taxiFareDeque.size(),
                RateLimiterStrategy.perSecond(200),
                TypeInformation.of(TaxiFare.class));
    }
}
