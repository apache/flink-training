package com.ververica.flink.training.provided;

import java.util.Objects;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class EnrichedMeasurement extends SimpleMeasurement {

	private float temperature;

	public EnrichedMeasurement() {
	}

	public EnrichedMeasurement(
			final int sensorId,
			final double value,
			final String location,
			final float temperature) {
		super(sensorId, value, location);
		this.temperature = temperature;
	}

	public EnrichedMeasurement(
			final SimpleMeasurement measurement,
			final float temperature) {
		super(measurement);
		this.temperature = temperature;
	}

	public float getTemperature() {
		return temperature;
	}

	public void setTemperature(float temperature) {
		this.temperature = temperature;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		EnrichedMeasurement that = (EnrichedMeasurement) o;
		return Float.compare(that.temperature, temperature) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), temperature);
	}

	@Override
	public String toString() {
		return "EnrichedMeasurement{" +
				super.toString() +
				", temperature=" + temperature +
				'}';
	}
}
