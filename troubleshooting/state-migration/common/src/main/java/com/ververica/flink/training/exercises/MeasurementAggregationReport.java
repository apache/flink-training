package com.ververica.flink.training.exercises;

/**
 * Type for reporting aggregate results.
 */
public class MeasurementAggregationReport {
	private int sensorId;
	private long count;
	private double average;
	private long latestUpdate;

	public MeasurementAggregationReport() {
	}

	public int getSensorId() {
		return sensorId;
	}

	public void setSensorId(int sensorId) {
		this.sensorId = sensorId;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	public long getLatestUpdate() {
		return latestUpdate;
	}

	public void setLatestUpdate(long latestUpdate) {
		this.latestUpdate = latestUpdate;
	}

	@Override
	public String toString() {
		return "MeasurementAggregationReport{" +
				"sensorId=" + sensorId +
				", count=" + count +
				", average=" + average +
				", latestUpdate=" + latestUpdate +
				'}';
	}
}
