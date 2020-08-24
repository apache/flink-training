package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;

import java.util.Objects;

@DoNotChangeThis
@SuppressWarnings({"WeakerAccess", "unused", "RedundantSuppression"})
public class ExtendedMeasurement {

	private Sensor sensor;
	private Location location;
	private MeasurementValue measurement;

	public ExtendedMeasurement() {
	}

	public ExtendedMeasurement(
			Sensor sensor,
			Location location,
			MeasurementValue measurement) {
		this.sensor = sensor;
		this.location = location;
		this.measurement = measurement;
	}

	public Sensor getSensor() {
		return sensor;
	}

	public void setSensor(Sensor sensor) {
		this.sensor = sensor;
	}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public MeasurementValue getMeasurement() {
		return measurement;
	}

	public void setMeasurement(MeasurementValue measurement) {
		this.measurement = measurement;
	}

	public enum SensorType {
		Temperature,
		Wind
	}

	public static class Sensor {
		private long sensorId;
		private long vendorId;
		private SensorType sensorType;

		public Sensor() {
		}

		public Sensor(
				long sensorId,
				long vendorId,
				SensorType sensorType) {
			this.sensorId = sensorId;
			this.vendorId = vendorId;
			this.sensorType = sensorType;
		}

		public long getSensorId() {
			return sensorId;
		}

		public void setSensorId(long sensorId) {
			this.sensorId = sensorId;
		}

		public long getVendorId() {
			return vendorId;
		}

		public void setVendorId(long vendorId) {
			this.vendorId = vendorId;
		}

		public SensorType getSensorType() {
			return sensorType;
		}

		public void setSensorType(SensorType sensorType) {
			this.sensorType = sensorType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Sensor sensor = (Sensor) o;
			return sensorId == sensor.sensorId &&
					vendorId == sensor.vendorId &&
					sensorType == sensor.sensorType;
		}

		@Override
		public int hashCode() {
			// NOTE: do not use the enum directly here. Why?
			// -> try with Sensor as a key in a distributed setting and see for yourself!
			return Objects.hash(sensorId, vendorId, sensorType.ordinal());
		}
	}

	public static class Location {
		private double longitude;
		private double latitude;
		private double height;

		public Location() {
		}

		public Location(double longitude, double latitude, double height) {
			this.longitude = longitude;
			this.latitude = latitude;
			this.height = height;
		}

		public double getLongitude() {
			return longitude;
		}

		public void setLongitude(double longitude) {
			this.longitude = longitude;
		}

		public double getLatitude() {
			return latitude;
		}

		public void setLatitude(double latitude) {
			this.latitude = latitude;
		}

		public double getHeight() {
			return height;
		}

		public void setHeight(double height) {
			this.height = height;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Location location = (Location) o;
			return Double.compare(location.longitude, longitude) == 0 &&
					Double.compare(location.latitude, latitude) == 0 &&
					Double.compare(location.height, height) == 0;
		}

		@Override
		public int hashCode() {
			return Objects.hash(longitude, latitude, height);
		}
	}

	public static class MeasurementValue {
		private double value;
		private float accuracy;
		private long timestamp;

		public MeasurementValue() {
		}

		public MeasurementValue(double value, float accuracy, long timestamp) {
			this.value = value;
			this.accuracy = accuracy;
			this.timestamp = timestamp;
		}

		public double getValue() {
			return value;
		}

		public void setValue(double value) {
			this.value = value;
		}

		public float getAccuracy() {
			return accuracy;
		}

		public void setAccuracy(float accuracy) {
			this.accuracy = accuracy;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
	}
}
