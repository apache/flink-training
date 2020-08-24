package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable variant of {@link com.ververica.flink.training.provided.ExtendedMeasurement.Sensor}.
 */
@SuppressWarnings("WeakerAccess")
@TypeInfo(Sensor.SensorTypeInfoFactory.class)
public class Sensor {
	public enum SensorType {
		Temperature,
		Wind
	}

	private final long sensorId;
	private final long vendorId;
	private final SensorType sensorType;

    /**
     * Constructor.
     */
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

	public long getVendorId() {
		return vendorId;
	}

	public SensorType getSensorType() {
		return sensorType;
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

	public static class SensorTypeInfoFactory extends TypeInfoFactory<Sensor> {
		@Override
		public TypeInformation<Sensor> createTypeInfo(
				Type t,
				Map<String, TypeInformation<?>> genericParameters) {
			return SensorTypeInfo.INSTANCE;
		}
	}
}
