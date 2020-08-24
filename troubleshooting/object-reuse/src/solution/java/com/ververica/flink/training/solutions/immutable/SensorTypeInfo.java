package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

class SensorTypeInfo extends TypeInformation<Sensor> {

	private SensorTypeInfo() {
	}

	static final SensorTypeInfo INSTANCE = new SensorTypeInfo();

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 3;
	}

	@Override
	public int getTotalFields() {
		return 3;
	}

	@Override
	public Class<Sensor> getTypeClass() {
		return Sensor.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<Sensor> createSerializer(ExecutionConfig config) {
		return SensorSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	@Override
	public boolean equals(Object obj) {
		return this.canEqual(obj);
	}

	@Override
	public int hashCode() {
		return Sensor.class.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof SensorTypeInfo;
	}
}
