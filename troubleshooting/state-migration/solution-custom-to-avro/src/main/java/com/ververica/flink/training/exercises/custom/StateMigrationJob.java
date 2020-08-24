package com.ververica.flink.training.exercises.custom;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.exercises.StateMigrationJobBase;

/**
 * State migration job for custom serializer state migration / schema evolution.
 */
@DoNotChangeThis
public class StateMigrationJob extends StateMigrationJobBase {

	/**
	 * Creates and starts the state migration streaming job.
	 *
	 * @throws Exception if the application is misconfigured or fails during job submission
	 */
	public static void main(String[] args) throws Exception {
		createAndExecuteJob(args, new SensorAggregationProcessing());
	}
}
