package com.ververica.flink.training.exercises.custom;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.training.statemigration.avro.AggregatedSensorStatistics;

import java.io.IOException;

public class AggregatedSensorStatisticsSerializerV2
		extends AggregatedSensorStatisticsSerializerBase<AggregatedSensorStatistics> {

	private static final long serialVersionUID = -6778106052344549131L;

	@Override
	public AggregatedSensorStatistics createInstance() {
		return new AggregatedSensorStatistics();
	}

	@Override
	public void serialize(AggregatedSensorStatistics record, DataOutputView target)
			throws IOException {
		target.writeInt(record.getSensorId());
		target.writeLong(record.getCount());
		target.writeDouble(record.getSum());
		target.writeLong(record.getLastUpdate());
	}

	@Override
	public AggregatedSensorStatistics deserialize(DataInputView source) throws IOException {
		int sensorId = source.readInt();
		long count = source.readLong();
		double sum = source.readDouble();
		long lastUpdate = source.readLong();

		AggregatedSensorStatistics result = new AggregatedSensorStatistics();
		result.setSensorId(sensorId);
		result.setCount(count);
		result.setSum(sum);
		result.setLastUpdate(lastUpdate);
		return result;
	}

	@Override
	public AggregatedSensorStatistics copy(AggregatedSensorStatistics from) {
		AggregatedSensorStatistics result = new AggregatedSensorStatistics();
		result.setSensorId(from.getSensorId());
		result.setCount(from.getCount());
		result.setSum(from.getSum());
		result.setLastUpdate(from.getLastUpdate());
		return result;
	}

	@Override
	public TypeSerializerSnapshot<AggregatedSensorStatistics> snapshotConfiguration() {
		return new AggregatedSensorStatisticsSerializerSnapshotV2();
	}
}
