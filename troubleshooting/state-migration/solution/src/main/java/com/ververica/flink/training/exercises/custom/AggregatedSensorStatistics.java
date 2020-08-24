package com.ververica.flink.training.exercises.custom;

/**
 * Sensor statistics state POJO.
 */
public class AggregatedSensorStatistics {
	private int sensorId;
	private long count;
	private double sum;
	private long lastUpdate;

	public AggregatedSensorStatistics() {
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

	public long getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(long lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}
}
