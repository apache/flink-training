package com.ververica.flink.training.exercises.custom;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class AggregatedSensorStatisticsSerializerV1
		extends AggregatedSensorStatisticsSerializerBase<AggregatedSensorStatistics> {

	private static final long serialVersionUID = 7089080352317847665L;

	@Override
	public AggregatedSensorStatistics createInstance() {
		return new AggregatedSensorStatistics();
	}

	@Override
	public void serialize(AggregatedSensorStatistics record, DataOutputView target)
			throws IOException {
		target.writeInt(record.getSensorId());
		target.writeLong(record.getCount());
		target.writeLong(record.getLastUpdate());
	}

	@Override
	public AggregatedSensorStatistics deserialize(DataInputView source) throws IOException {
		int sensorId = source.readInt();
		long count = source.readLong();
		long lastUpdate = source.readLong();

		AggregatedSensorStatistics result = new AggregatedSensorStatistics();
		result.setSensorId(sensorId);
		result.setCount(count);
		result.setLastUpdate(lastUpdate);
		return result;
	}

	@Override
	public AggregatedSensorStatistics copy(AggregatedSensorStatistics from) {
		AggregatedSensorStatistics result = new AggregatedSensorStatistics();
		result.setSensorId(from.getSensorId());
		result.setCount(from.getCount());
		result.setLastUpdate(from.getLastUpdate());
		return result;
	}

	@Override
	public TypeSerializerSnapshot<AggregatedSensorStatistics> snapshotConfiguration() {
		return new AggregatedSensorStatisticsSerializerSnapshotV1();
	}
}
