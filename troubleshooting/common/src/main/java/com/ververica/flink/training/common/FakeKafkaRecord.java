package com.ververica.flink.training.common;

import java.util.Arrays;
import java.util.Objects;

@DoNotChangeThis
@SuppressWarnings({"unused", "RedundantSuppression"})
public class FakeKafkaRecord {

	private long timestamp;
	private byte[] key;
	private byte[] value;
	private int partition;

	public FakeKafkaRecord() {
	}

	public FakeKafkaRecord(
			final long timestamp,
			final byte[] key,
			final byte[] value,
			final int partition) {
		this.timestamp = timestamp;
		this.key = key;
		this.value = value;
		this.partition = partition;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(final long timestamp) {
		this.timestamp = timestamp;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(final byte[] key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(final byte[] value) {
		this.value = value;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(final int partition) {
		this.partition = partition;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final FakeKafkaRecord that = (FakeKafkaRecord) o;
		return timestamp == that.timestamp &&
				partition == that.partition &&
				Arrays.equals(key, that.key) &&
				Arrays.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(timestamp, partition);
		result = 31 * result + Arrays.hashCode(key);
		result = 31 * result + Arrays.hashCode(value);
		return result;
	}

	@Override
	public String toString() {
		return "FakeKafkaRecord{" + "timestamp=" + timestamp +
				", key=" + Arrays.toString(key) +
				", value=" + Arrays.toString(value) +
				", partition=" + partition +
				'}';
	}
}
