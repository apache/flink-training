package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.Measurement;
import com.ververica.flink.training.common.SourceUtils;
import com.ververica.flink.training.common.WindowedMeasurements;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * Solution 2 for the streaming job with slow checkpointing
 * (Solution 1 does not need any code fixes; do you know what can be done there?).
 */
public class CheckpointingJobSolution2 {

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

		//Checkpointing Configuration (use cluster-configs if not run locally)
		if (isLocal(parameters)) {
			env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
			env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(2));
		}

		DataStream<Tuple2<Measurement, Long>> sourceStream = env
				.addSource(SourceUtils.createFailureFreeFakeKafkaSource())
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

		DataStream<WindowedMeasurements> aggregatedPerLocation = sourceStream
				.keyBy(x -> x.f0.getSensorId())
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.MINUTES), Time.of(1, TimeUnit.SECONDS)))
				.process(new MeasurementWindowProcessFunction())
				.name("WindowedAggregationPerLocation")
				.uid("WindowedAggregationPerLocation");

		if (isLocal(parameters)) {
			aggregatedPerLocation.print()
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
		} else {
			aggregatedPerLocation.addSink(new DiscardingSink<>())
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
		}

		env.execute(CheckpointingJobSolution2.class.getSimpleName());
	}

	/**
	 * Deserializes the JSON Kafka message.
	 */
	public static class MeasurementDeserializer extends
			RichFlatMapFunction<FakeKafkaRecord, Tuple2<Measurement, Long>> {
		private static final long serialVersionUID = 3L;

		private Counter numInvalidRecords;
		private transient ObjectMapper instance;

		@Override
		public void open(final Configuration parameters) throws Exception {
			super.open(parameters);
			numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
			instance = createObjectMapper();
		}

		@Override
		public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Tuple2<Measurement, Long>> out) {
			final Measurement node;
			try {
				node = deserialize(kafkaRecord.getValue());
			} catch (IOException e) {
				numInvalidRecords.inc();
				return;
			}
			out.collect(Tuple2.of(node, kafkaRecord.getTimestamp()));
		}

		private Measurement deserialize(final byte[] bytes) throws IOException {
			return instance.readValue(bytes, Measurement.class);
		}
	}

	private static class MeasurementByTimeComparator implements Comparator<Tuple2<Measurement, Long>> {
		@Override
		public int compare(Tuple2<Measurement, Long> o1, Tuple2<Measurement, Long> o2) {
			return Long.compare(o1.f1, o2.f1);
		}
	}

	/**
	 * Calculates data for retrieving the average temperature difference between two sensor readings
	 * (in event-time order!).
	 */
	public static class MeasurementWindowProcessFunction
			extends
			ProcessWindowFunction<Tuple2<Measurement, Long>, WindowedMeasurements, Integer, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

		private transient DescriptiveStatisticsHistogram eventTimeLag;

		@Override
		public void process(
				final Integer sensorId,
				final Context context,
				final Iterable<Tuple2<Measurement, Long>> input,
				final Collector<WindowedMeasurements> out) {

			ArrayList<Tuple2<Measurement, Long>> list = new ArrayList<>();
			input.iterator().forEachRemaining(list::add);
			list.sort(new MeasurementByTimeComparator());

			long eventsPerWindow = 0L;
			double sumPerWindow = 0.0;
			Measurement previous = null;
			for (Tuple2<Measurement, Long> measurement : list) {
				++eventsPerWindow;
				if (previous != null) {
					sumPerWindow += measurement.f0.getValue() - previous.getValue();
				}
				previous = measurement.f0;
			}

			final TimeWindow window = context.window();
			WindowedMeasurements result = new WindowedMeasurements();
			result.setEventsPerWindow(eventsPerWindow);
			result.setSumPerWindow(sumPerWindow);
			result.setWindow(window);
			result.setLocation(sensorId.toString());

			eventTimeLag.update(System.currentTimeMillis() - window.getEnd());
			out.collect(result);
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
