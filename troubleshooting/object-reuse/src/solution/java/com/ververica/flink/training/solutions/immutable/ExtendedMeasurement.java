package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Immutable variant of {@link com.ververica.flink.training.provided.ExtendedMeasurement}.
 */
@TypeInfo(ExtendedMeasurement.ExtendedMeasurementTypeInfoFactory.class)
public class ExtendedMeasurement {

	private final Sensor sensor;
	private final Location location;
	private final MeasurementValue measurement;

	/**
	 * Constructor.
	 */
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

	public Location getLocation() {
		return location;
	}

	public MeasurementValue getMeasurement() {
		return measurement;
	}

	public static class ExtendedMeasurementTypeInfoFactory extends
			TypeInfoFactory<ExtendedMeasurement> {
		@Override
		public TypeInformation<ExtendedMeasurement> createTypeInfo(
				Type t,
				Map<String, TypeInformation<?>> genericParameters) {
			return ExtendedMeasurementTypeInfo.INSTANCE;
		}
	}
}
