package com.ververica.flink.training.exercises.avro;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.ververica.flink.training.common.Measurement;
import com.ververica.flink.training.exercises.MeasurementAggregationReport;
import com.ververica.flink.training.exercises.SensorAggregationProcessingBase;
import com.ververica.flink.training.exercises.StateMigrationJobBase;
import com.ververica.training.statemigration.avro.AggregatedSensorStatistics;

/**
 * Process function to report aggregated sensor statistics using Avro-serialized state.
 */
public class SensorAggregationProcessing extends SensorAggregationProcessingBase {

	private static final int REPORTING_INTERVAL = 60_000;

	private static final long serialVersionUID = 4123696380484855346L;

	private transient ValueState<AggregatedSensorStatistics> aggregationState;

	@Override
	public void processElement(
			Measurement measurement,
			Context ctx,
			Collector<MeasurementAggregationReport> out) throws Exception {
		if (ctx.timestamp() > ctx.timerService().currentWatermark()) {
			AggregatedSensorStatistics currentStats = aggregationState.value();
			if (currentStats == null) {
				currentStats = AggregatedSensorStatistics.newBuilder()
						.setSensorId(measurement.getSensorId())
						.build();
			}
			currentStats.setCount(currentStats.getCount() + 1);
			currentStats.setSum(currentStats.getSum() + measurement.getValue());

			// emit once per minute
			long reportingTime = (ctx.timestamp() / REPORTING_INTERVAL) * REPORTING_INTERVAL;
			if (reportingTime > ctx.timerService().currentWatermark()) {
				ctx.timerService().registerEventTimeTimer(reportingTime);
			}

			aggregationState.update(currentStats);
		} else {
			ctx.output(StateMigrationJobBase.LATE_DATA_TAG, measurement);
		}
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<MeasurementAggregationReport> out) throws Exception {
		AggregatedSensorStatistics currentStats = aggregationState.value();
		currentStats.setLastUpdate(ctx.timestamp());

		MeasurementAggregationReport report = new MeasurementAggregationReport();
		report.setSensorId(currentStats.getSensorId());
		report.setCount(currentStats.getCount());
		report.setAverage(currentStats.getSum() / (double) currentStats.getCount());
		report.setLatestUpdate(currentStats.getLastUpdate());

		out.collect(report);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		final ValueStateDescriptor<AggregatedSensorStatistics> aggregationStateDesc =
				new ValueStateDescriptor<>("aggregationStats", AggregatedSensorStatistics.class);
		aggregationState = getRuntimeContext().getState(aggregationStateDesc);
	}

	@Override
	public String getStateSerializerName() {
		return "avro v2";
	}
}
