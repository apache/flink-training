package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class LocationSerializer extends TypeSerializerSingleton<Location> {

	private LocationSerializer() {
	}

	static final LocationSerializer INSTANCE = new LocationSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Location createInstance() {
		return null;
	}

	@Override
	public Location copy(Location from) {
		return new Location(from.getLongitude(), from.getLatitude(), from.getHeight());
	}

	@Override
	public Location copy(Location from, Location reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return Double.BYTES + Double.BYTES + Double.BYTES;
	}

	@Override
	public void serialize(Location record, DataOutputView target) throws IOException {
		target.writeDouble(record.getLongitude());
		target.writeDouble(record.getLatitude());
		target.writeDouble(record.getHeight());
	}

	@Override
	public Location deserialize(DataInputView source) throws IOException {
		double longitude = source.readDouble();
		double latitude = source.readDouble();
		double height = source.readDouble();
		return new Location(longitude, latitude, height);
	}

	@Override
	public Location deserialize(Location reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeDouble(source.readDouble());
		target.writeDouble(source.readDouble());
		target.writeDouble(source.readDouble());
	}

	// -----------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Location> snapshotConfiguration() {
		return new LocationSerializerSnapshot();
	}

	@SuppressWarnings("WeakerAccess")
	public static final class LocationSerializerSnapshot extends
			SimpleTypeSerializerSnapshot<Location> {

		/**
		 * Returns a snapshot pointing to the singleton serializer instance.
		 */
		public LocationSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
