package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.SourceUtils;
import com.ververica.flink.training.common.WindowedMeasurements;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * Troubled streaming job evolved on top of {@link TroubledStreamingJobSolution42} by optimising
 * throughput removing unnecessary POJO fields.
 */
public class TroubledStreamingJobSolution43 {

    /**
     * Creates and starts the troubled streaming job.
	 *
	 * @throws Exception if the application is misconfigured or fails during job submission
     */
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

		//Timing Configuration
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
						new MeasurementTSExtractor(Time.of(250, TimeUnit.MILLISECONDS),
								Time.of(1, TimeUnit.SECONDS)))
				.name("Watermarks")
				.uid("Watermarks")
				.flatMap(new MeasurementDeserializer())
				.name("Deserialization")
				.uid("Deserialization");

		OutputTag<SimpleMeasurement> lateDataTag = new OutputTag<SimpleMeasurement>("late-data") {
			private static final long serialVersionUID = 33513631677208956L;
		};

		SingleOutputStreamOperator<WindowedMeasurements> aggregatedPerLocation = sourceStream
				.keyBy(SimpleMeasurement::getLocation)
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				.sideOutputLateData(lateDataTag)
				.aggregate(new MeasurementWindowAggregatingFunction(),
						new MeasurementWindowProcessFunction())
				.name("WindowedAggregationPerLocation")
				.uid("WindowedAggregationPerLocation");

		if (isLocal(parameters)) {
			aggregatedPerLocation.print()
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
			aggregatedPerLocation.getSideOutput(lateDataTag).printToErr()
					.name("LateDataSink")
					.uid("LateDataSink")
					.disableChaining();
		} else {
			aggregatedPerLocation.addSink(new DiscardingSink<>())
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
			aggregatedPerLocation.getSideOutput(lateDataTag).addSink(new DiscardingSink<>())
					.name("LateDataSink")
					.uid("LateDataSink")
					.disableChaining();
		}

		env.execute(TroubledStreamingJobSolution43.class.getSimpleName());
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
			instance = createObjectMapper();
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

	public static class MeasurementTSExtractor implements
			AssignerWithPeriodicWatermarks<FakeKafkaRecord> {
		private static final long serialVersionUID = 2L;

		private long currentMaxTimestamp;
		private long lastEmittedWatermark = Long.MIN_VALUE;
		private long lastRecordProcessingTime;

		private final long maxOutOfOrderness;
		private final long idleTimeout;

		MeasurementTSExtractor(Time maxOutOfOrderness, Time idleTimeout) {
			if (maxOutOfOrderness.toMilliseconds() < 0) {
				throw new RuntimeException("Tried to set the maximum allowed " +
						"lateness to " + maxOutOfOrderness +
						". This parameter cannot be negative.");
			}

			if (idleTimeout.toMilliseconds() < 0) {
				throw new RuntimeException("Tried to set the idle Timeout" + idleTimeout +
						". This parameter cannot be negative.");
			}

			this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
			this.idleTimeout = idleTimeout.toMilliseconds();
			this.currentMaxTimestamp = Long.MIN_VALUE;
		}

		/**
		 * Gets the configured out of orderness (in ms).
		 */
		public long getMaxOutOfOrdernessInMillis() {
			return maxOutOfOrderness;
		}

		@Override
		public final Watermark getCurrentWatermark() {

			// if last record was processed more than the idleTimeout in the past, consider this
			// source idle and set timestamp to current processing time
			long currentProcessingTime = System.currentTimeMillis();
			if (lastRecordProcessingTime < currentProcessingTime - idleTimeout) {
				this.currentMaxTimestamp = currentProcessingTime;
			}

			long potentialWM = this.currentMaxTimestamp - maxOutOfOrderness;
			if (potentialWM >= lastEmittedWatermark) {
				lastEmittedWatermark = potentialWM;
			}
			return new Watermark(lastEmittedWatermark);
		}

		@Override
		public final long extractTimestamp(FakeKafkaRecord element, long previousElementTimestamp) {
			lastRecordProcessingTime = System.currentTimeMillis();
			long timestamp = element.getTimestamp();
			if (timestamp > currentMaxTimestamp) {
				currentMaxTimestamp = timestamp;
			}
			return timestamp;
		}
	}

	public static class MeasurementWindowAggregatingFunction
			implements
			AggregateFunction<SimpleMeasurement, WindowedMeasurements, WindowedMeasurements> {
		private static final long serialVersionUID = -1083906142198231377L;

		@Override
		public WindowedMeasurements createAccumulator() {
			return new WindowedMeasurements();
		}

		@Override
		public WindowedMeasurements add(
				final SimpleMeasurement record,
				final WindowedMeasurements aggregate) {
			double result = record.getValue();
			aggregate.addMeasurement(result);
			return aggregate;
		}

		@Override
		public WindowedMeasurements getResult(final WindowedMeasurements windowedMeasurements) {
			return windowedMeasurements;
		}

		@Override
		public WindowedMeasurements merge(
				final WindowedMeasurements agg1,
				final WindowedMeasurements agg2) {
			agg2.setEventsPerWindow(agg1.getEventsPerWindow() + agg2.getEventsPerWindow());
			agg2.setSumPerWindow(agg1.getSumPerWindow() + agg2.getSumPerWindow());
			return agg2;
		}
	}

	public static class MeasurementWindowProcessFunction
			extends
			ProcessWindowFunction<WindowedMeasurements, WindowedMeasurements, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

		private transient DescriptiveStatisticsHistogram eventTimeLag;

		@Override
		public void process(
				final String location,
				final Context context,
				final Iterable<WindowedMeasurements> input,
				final Collector<WindowedMeasurements> out) {

			// Windows with pre-aggregation only forward the final to the WindowFunction
			WindowedMeasurements aggregate = input.iterator().next();

			final TimeWindow window = context.window();
			aggregate.setWindow(window);
			aggregate.setLocation(location);

			eventTimeLag.update(System.currentTimeMillis() - window.getEnd());
			out.collect(aggregate);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			eventTimeLag = getRuntimeContext().getMetricGroup().histogram("eventTimeLag",
					new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
		}
	}

	private static ObjectMapper createObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return objectMapper;
	}
}
