package com.ververica.flink.training.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The {@link FakeKafkaSource} reads from {@code NO_OF_PARTIONS} Kafka partitions.
 *
 * <p>The timestamps roughly start at the epoch and are ascending per partition. The partitions themselves can be out of sync.
 **
 */
@DoNotChangeThis
public class FakeKafkaSource extends
		RichParallelSourceFunction<FakeKafkaRecord> implements
		CheckpointedFunction {
	private static final long serialVersionUID = 4658785571367840693L;

	private static final int NO_OF_PARTIONS = 8;
	public static final Logger LOG = LoggerFactory.getLogger(FakeKafkaSource.class);

	private final Random rand;

	private transient volatile boolean cancelled;
	private transient int indexOfThisSubtask;
	private transient int numberOfParallelSubtasks;
	private transient List<Integer> assignedPartitions;

	private final List<byte[]> serializedMeasurements;
	private final double poisonPillRate;
	private final BitSet idlePartitions;

	FakeKafkaSource(final int seed, final float poisonPillRate, List<Integer> idlePartitions,
					List<byte[]> serializedMeasurements) {
		this.poisonPillRate = poisonPillRate;
		this.idlePartitions = new BitSet(NO_OF_PARTIONS);
		for (int i : idlePartitions) {
			this.idlePartitions.set(i);
		}
		this.serializedMeasurements = serializedMeasurements;

		this.rand = new Random(seed);
	}

	@Override
	public void open(final Configuration parameters) {
		indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
		numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

		assignedPartitions = IntStream.range(0, NO_OF_PARTIONS)
				.filter(i -> i % numberOfParallelSubtasks == indexOfThisSubtask)
				.boxed()
				.collect(Collectors.toList());

		LOG.info("Now reading from partitions: {}", assignedPartitions);
	}

	@Override
	public void run(
			final SourceContext<FakeKafkaRecord> sourceContext)
			throws Exception {

		int numberOfPartitions = assignedPartitions.size();

		if (!assignedPartitions.isEmpty()) {
			while (!cancelled) {
				int nextPartition = assignedPartitions.get(rand.nextInt(numberOfPartitions));

				if (idlePartitions.get(nextPartition)) {
					//noinspection BusyWait
					Thread.sleep(1000); // avoid spinning wait
					continue;
				}

				long nextTimestamp = getTimestampForPartition(nextPartition);

				byte[] serializedMeasurement =
						serializedMeasurements.get(rand.nextInt(serializedMeasurements.size()));

				if (rand.nextFloat() > 1 - poisonPillRate) {
					serializedMeasurement = Arrays.copyOf(serializedMeasurement, 10);
				}

				synchronized (sourceContext.getCheckpointLock()) {
					sourceContext.collect(
							new FakeKafkaRecord(
									nextTimestamp, null, serializedMeasurement, nextPartition));
				}
			}
		} else {
			// this source doesn't have any partitions and thus never emits any records
			// (and therefore also no watermarks), so we mark this subtask as idle to
			// not block watermark forwarding
			sourceContext.markAsTemporarilyIdle();

			// wait until this is canceled
			final Object waitLock = new Object();
			while (!cancelled) {
				try {
					//noinspection SynchronizationOnLocalVariableOrMethodParameter
					synchronized (waitLock) {
						waitLock.wait();
					}
				} catch (InterruptedException e) {
					if (cancelled) {
						// restore the interrupted state, and fall through the loop
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}

	private long getTimestampForPartition(int partition) {
		return System.currentTimeMillis() - (partition * 50L);
	}

	@Override
	public void cancel() {
		cancelled = true;

		// there will be an interrupt() call to the main thread anyways
	}

	@Override
	public void snapshotState(final FunctionSnapshotContext context) {
	}

	@Override
	public void initializeState(final FunctionInitializationContext context) {
	}
}
