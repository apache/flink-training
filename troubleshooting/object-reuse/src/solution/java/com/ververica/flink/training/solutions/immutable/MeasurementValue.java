package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Immutable variant of {@link com.ververica.flink.training.provided.ExtendedMeasurement.MeasurementValue}.
 */
@TypeInfo(MeasurementValue.MeasurementValueTypeInfoFactory.class)
public class MeasurementValue {
	private final double value;
	private final float accuracy;
	private final long timestamp;

	/**
	 * Constructor.
	 */
	public MeasurementValue(double value, float accuracy, long timestamp) {
		this.value = value;
		this.accuracy = accuracy;
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public float getAccuracy() {
		return accuracy;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public static class MeasurementValueTypeInfoFactory extends TypeInfoFactory<MeasurementValue> {
		@Override
		public TypeInformation<MeasurementValue> createTypeInfo(
				Type t,
				Map<String, TypeInformation<?>> genericParameters) {
			return MeasurementValueTypeInfo.INSTANCE;
		}
	}
}
