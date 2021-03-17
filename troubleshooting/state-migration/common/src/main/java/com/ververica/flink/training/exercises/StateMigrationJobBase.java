package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.EnvironmentUtils;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.Measurement;
import com.ververica.flink.training.common.SourceUtils;

import java.time.Duration;

@DoNotChangeThis
public class StateMigrationJobBase {
	public static final OutputTag<Measurement> LATE_DATA_TAG =
			new OutputTag<Measurement>("late-data") {
				private static final long serialVersionUID = 33513631677208956L;
			};

	protected static void createAndExecuteJob(
			String[] args,
			SensorAggregationProcessingBase keyedProcessFunction)
			throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);

		//Checkpointing Configuration
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

		DataStream<Measurement> sourceStream = env
				.addSource(SourceUtils.createFailureFreeFakeKafkaSource()).name("FakeKafkaSource")
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<FakeKafkaRecord>forBoundedOutOfOrderness(
								Duration.ofMillis(250))
								.withTimestampAssigner(
										(element, timestamp) -> element.getTimestamp())
				)
				.map(new MeasurementDeserializer())
				.name("Deserialization");

		SingleOutputStreamOperator<MeasurementAggregationReport> aggregatedPerSensor = sourceStream
				.keyBy(Measurement::getSensorId)
				.process(keyedProcessFunction)
				.name("AggregatePerSensor (" + keyedProcessFunction.getStateSerializerName() + ")")
				.uid("AggregatePerSensor");

		aggregatedPerSensor.addSink(new DiscardingSink<>()).name("NormalOutput")
				.disableChaining();
		aggregatedPerSensor.getSideOutput(LATE_DATA_TAG).addSink(new DiscardingSink<>())
				.name("LateDataSink").disableChaining();

		env.execute();
	}

	/**
	 * Deserializes the JSON Kafka message.
	 */
	public static class MeasurementDeserializer extends
			RichMapFunction<FakeKafkaRecord, Measurement> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper mapper;

		@Override
		public void open(final Configuration parameters) throws Exception {
			super.open(parameters);
			mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		}

		@Override
		public Measurement map(FakeKafkaRecord kafkaRecord) throws Exception {
			return mapper.readValue(kafkaRecord.getValue(), Measurement.class);
		}

	}

}
