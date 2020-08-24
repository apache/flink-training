package com.ververica.flink.training.exercises.custom;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.flink.training.common.DoNotChangeThis;

import java.io.IOException;

/**
 * Custom serializer base class providing some default implementations for convenience.
 */
@DoNotChangeThis
public abstract class AggregatedSensorStatisticsSerializerBase<T> extends
		TypeSerializer<T> {
	private static final long serialVersionUID = -6967834481356870922L;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public T deserialize(
			T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public T copy(T from, T reuse) {
		return copy(from);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public TypeSerializer<T> duplicate() {
		return this;
	}

	@Override
	public int hashCode() {
		return this.getClass().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AggregatedSensorStatisticsSerializerBase) {
			AggregatedSensorStatisticsSerializerBase<?> other =
					(AggregatedSensorStatisticsSerializerBase<?>) obj;

			return other.getClass().equals(getClass());
		} else {
			return false;
		}
	}
}
