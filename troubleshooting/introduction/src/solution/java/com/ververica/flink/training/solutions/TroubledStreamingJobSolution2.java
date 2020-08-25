package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.SourceUtils;
import com.ververica.flink.training.common.WindowedMeasurements;

import java.io.IOException;
import java.time.Duration;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * Troubled streaming job evolved on top of {@link TroubledStreamingJobSolution1} by fixing the
 * watermarking to cope with idle partitions.
 */
public class TroubledStreamingJobSolution2 {

    /**
     * Creates and starts the troubled streaming job.
	 *
	 * @throws Exception if the application is misconfigured or fails during job submission
     */
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

		//Timing Configuration
		env.getConfig().setAutoWatermarkInterval(2000);

		//Checkpointing Configuration
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

		DataStream<JsonNode> sourceStream = env
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

		OutputTag<JsonNode> lateDataTag = new OutputTag<JsonNode>("late-data") {
			private static final long serialVersionUID = 33513631677208956L;
		};

		SingleOutputStreamOperator<WindowedMeasurements> aggregatedPerLocation = sourceStream
				.keyBy(jsonNode -> jsonNode.get("location").asText())
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				.sideOutputLateData(lateDataTag)
				.process(new MeasurementWindowAggregatingFunction())
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

		env.execute(TroubledStreamingJobSolution2.class.getSimpleName());
	}

	/**
	 * Deserializes the JSON Kafka message.
	 */
	public static class MeasurementDeserializer extends
			RichFlatMapFunction<FakeKafkaRecord, JsonNode> {
		private static final long serialVersionUID = 2L;

		private Counter numInvalidRecords;

		@Override
		public void open(final Configuration parameters) throws Exception {
			super.open(parameters);
			numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
		}

		@Override
		public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<JsonNode> out) {
			final JsonNode node;
			try {
				node = deserialize(kafkaRecord.getValue());
			} catch (IOException e) {
				numInvalidRecords.inc();
				return;
			}
			out.collect(node);
		}

		private JsonNode deserialize(final byte[] bytes) throws IOException {
			return ObjectMapperSingleton.getInstance().readValue(bytes, JsonNode.class);
		}
	}

	public static class MeasurementWindowAggregatingFunction
			extends ProcessWindowFunction<JsonNode, WindowedMeasurements, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

		private transient DescriptiveStatisticsHistogram eventTimeLag;

		@Override
		public void process(
				final String location,
				final Context context,
				final Iterable<JsonNode> input,
				final Collector<WindowedMeasurements> out) {

			WindowedMeasurements aggregate = new WindowedMeasurements();
			for (JsonNode record : input) {
				double result = Double.parseDouble(record.get("value").asText());
				aggregate.addMeasurement(result);
			}

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

	private static class ObjectMapperSingleton {
		static ObjectMapper getInstance() {
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			return objectMapper;
		}
	}
}
