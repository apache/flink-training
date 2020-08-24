package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.SourceUtils;
import com.ververica.flink.training.provided.EnrichedMeasurement;
import com.ververica.flink.training.provided.SimpleMeasurement;
import com.ververica.flink.training.provided.TemperatureClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * Troubled streaming job that enriches data from an external component.
 */
public class ExternalEnrichmentJobSolution {

	/**
	 * Creates and starts the troubled streaming job.
	 *
	 * @throws Exception if the application is misconfigured or fails during job submission
	 */
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

		//Time Characteristics
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(100);
		env.setBufferTimeout(10);

		//Checkpointing Configuration
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

		DataStream<SimpleMeasurement> sourceStream = env
				.addSource(SourceUtils.createFakeKafkaSource())
				.name("FakeKafkaSource")
				.uid("FakeKafkaSource")
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<FakeKafkaRecord>forBoundedOutOfOrderness(Duration.ofMillis(250))
								.withTimestampAssigner(
										(element, timestamp) -> element.getTimestamp())
								.withIdleness(Duration.ofSeconds(1)))
				.name("Watermarks")
				.uid("Watermarks")
				.flatMap(new MeasurementDeserializer())
				.name("Deserialization")
				.uid("Deserialization");

		DataStream<EnrichedMeasurement> enrichedStream = AsyncDataStream.unorderedWait(
				sourceStream.keyBy(SimpleMeasurement::getLocation),
				new EnrichMeasurementWithTemperatureAsync(10000),
				0,
				TimeUnit.MILLISECONDS,
				20)
				.name("Enrichment");

		if (isLocal(parameters)) {
			enrichedStream.print()
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
		} else {
			enrichedStream.addSink(new DiscardingSink<>())
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
		}

		env.execute(ExternalEnrichmentJobSolution.class.getSimpleName());
	}

	/**
	 * Deserializes the JSON Kafka message.
	 */
	public static class MeasurementDeserializer extends
			RichFlatMapFunction<FakeKafkaRecord, SimpleMeasurement> {
		private static final long serialVersionUID = 4L;

		private Counter numInvalidRecords;
		private transient ObjectMapper instance;

		@Override
		public void open(final Configuration parameters) throws Exception {
			super.open(parameters);
			numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			instance = objectMapper;
		}

		@Override
		public void flatMap(
				final FakeKafkaRecord kafkaRecord,
				final Collector<SimpleMeasurement> out) {
			final SimpleMeasurement node;
			try {
				node = deserialize(kafkaRecord.getValue());
			} catch (IOException e) {
				numInvalidRecords.inc();
				return;
			}
			out.collect(node);
		}

		private SimpleMeasurement deserialize(final byte[] bytes) throws IOException {
			return instance.readValue(bytes, SimpleMeasurement.class);
		}
	}

	public static class EnrichMeasurementWithTemperatureAsync extends
			RichAsyncFunction<SimpleMeasurement, EnrichedMeasurement> {
		private static final long serialVersionUID = 2L;

		private transient TemperatureClient temperatureClient;
		private transient Map<String, TemperatureCacheEntry> cache;

		private final int cacheExpiryMs;
		private Counter cacheSizeMetric;
		private Counter servedFromCacheMetric;

		/**
		 * Creates a new enrichment function with a (local) cache that expires after the given
		 * number of milliseconds.
		 */
		public EnrichMeasurementWithTemperatureAsync(int cacheExpiryMs) {
			this.cacheExpiryMs = cacheExpiryMs;
		}

		@Override
		public void open(final Configuration parameters) {
			temperatureClient = new TemperatureClient();
			cache = new HashMap<>();
			servedFromCacheMetric = getRuntimeContext().getMetricGroup()
					.counter("temperatureRequestsServedFromCache");
			cacheSizeMetric = getRuntimeContext().getMetricGroup().counter("temperatureCacheSize");
		}

		@Override
		public void asyncInvoke(
				SimpleMeasurement measurement,
				ResultFuture<EnrichedMeasurement> resultFuture) {
			String location = measurement.getLocation();
			final float temperature;

			TemperatureCacheEntry cachedTemperature = cache.get(location);
			if (cachedTemperature != null && !cachedTemperature.isTooOld(cacheExpiryMs)) {
				temperature = cachedTemperature.value;
				EnrichedMeasurement enrichedMeasurement =
						new EnrichedMeasurement(measurement, temperature);
				resultFuture.complete(Collections.singleton(enrichedMeasurement));
				servedFromCacheMetric.inc();
			} else {
				temperatureClient.asyncGetTemperatureFor(
						measurement.getLocation(),
						new TemperatureCallBack(resultFuture, measurement, location));
			}
		}

		private static class TemperatureCacheEntry {
			long timestamp;
			float value;

			/**
			 * Creates a new temperature cache entry.
			 */
			public TemperatureCacheEntry(final long timestamp, final float value) {
				this.timestamp = timestamp;
				this.value = value;
			}

			/**
			 * Returns <code>true</code> if the entry was created more than <code>expiryMs</code>
			 * milliseconds ago.
			 */
			public boolean isTooOld(int expiryMs) {
				return System.currentTimeMillis() - timestamp >= expiryMs;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o) {
					return true;
				}
				if (o == null || getClass() != o.getClass()) {
					return false;
				}
				TemperatureCacheEntry that = (TemperatureCacheEntry) o;
				return timestamp == that.timestamp &&
						Float.compare(that.value, value) == 0;
			}

			@Override
			public int hashCode() {
				return Objects.hash(timestamp, value);
			}

			@Override
			public String toString() {
				return "TemperatureCacheEntry{" +
						"timestamp=" + timestamp +
						", value=" + value +
						'}';
			}
		}

		private class TemperatureCallBack implements Consumer<Float> {
			private final ResultFuture<EnrichedMeasurement> resultFuture;
			private final SimpleMeasurement measurement;
			private final String location;

			public TemperatureCallBack(
					final ResultFuture<EnrichedMeasurement> resultFuture,
					final SimpleMeasurement measurement, final String location) {
				this.resultFuture = resultFuture;
				this.measurement = measurement;
				this.location = location;
			}

			@Override
			public void accept(final Float temperature) {
				EnrichedMeasurement enrichedMeasurement =
						new EnrichedMeasurement(measurement, temperature);
				resultFuture.complete(Collections.singleton(enrichedMeasurement));

				TemperatureCacheEntry newEntry =
						new TemperatureCacheEntry(System.currentTimeMillis(), temperature);
				if (cache.put(location, newEntry) == null) {
					cacheSizeMetric.inc();
				}
			}
		}
	}
}
