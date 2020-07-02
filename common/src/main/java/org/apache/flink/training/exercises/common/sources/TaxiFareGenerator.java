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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

/**
 * This SourceFunction generates a data stream of TaxiFare records that include event time
 * timestamps.
 *
 * <p>The stream is generated in order, and it includes Watermarks.
 *
 */
public class TaxiFareGenerator implements SourceFunction<TaxiFare> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<TaxiFare> ctx) throws Exception {

		long id = 1;

		while (running) {
			TaxiFare fare = new TaxiFare(id);
			id += 1;

			ctx.collectWithTimestamp(fare, fare.getEventTime());
			ctx.emitWatermark(new Watermark(fare.getEventTime()));

			// match our event production rate to that of the TaxiRideGenerator
			Thread.sleep(TaxiRideGenerator.SLEEP_MILLIS_PER_EVENT);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
