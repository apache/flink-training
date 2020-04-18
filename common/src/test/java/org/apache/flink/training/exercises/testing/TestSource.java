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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class TestSource<T> implements SourceFunction<T> {
	private volatile boolean running = true;
	// T or watermark (Long)
	protected Object[] testStream;

	@Override
	public void run(SourceContext<T> ctx) {
		for (int i = 0; (i < testStream.length) && running; i++) {
			if (testStream[i] instanceof Long) {
				Long ts = (Long) testStream[i];
				ctx.emitWatermark(new Watermark(ts));
			} else {
				//noinspection unchecked
				T element = (T) testStream[i];
				ctx.collectWithTimestamp(element, getTimestamp(element));
			}
		}
		// test sources are finite, so they have a Long.MAX_VALUE watermark when they finishes
	}

	abstract long getTimestamp(T element);

	@Override
	public void cancel() {
		running = false;
	}
}
