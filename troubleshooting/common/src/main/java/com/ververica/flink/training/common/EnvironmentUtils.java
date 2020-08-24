package com.ververica.flink.training.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * Common functionality to set up execution environments for the troubleshooting training.
 */
public class EnvironmentUtils {
	/**
	 * Creates a streaming environment with a few pre-configured settings based on command-line
	 * parameters.
	 *
	 * @throws IOException        if the local checkpoint directory for the file system state backend cannot be created
	 * @throws URISyntaxException if <code>fsStatePath</code> is not a valid URI
	 */
	public static StreamExecutionEnvironment createConfiguredEnvironment(
			final ParameterTool parameters) throws
			IOException, URISyntaxException {
		final boolean local = isLocal(parameters);

		StreamExecutionEnvironment env;
		if (local) {
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

			String statePath = parameters.get("fsStatePath");
			Path checkpointPath;
			if (statePath != null) {
				FileUtils.deleteDirectory(new File(new URI(statePath)));
				checkpointPath = Path.fromLocalFile(new File(new URI(statePath)));
			} else {
				checkpointPath =
						Path.fromLocalFile(Files.createTempDirectory("checkpoints").toFile());
			}

			final StateBackend stateBackend;
			if (parameters.has("useRocksDB")) {
				stateBackend = new RocksDBStateBackend(checkpointPath.toUri());
			} else {
				stateBackend = new FsStateBackend(checkpointPath);
			}
			env.setStateBackend(stateBackend);
		} else {
			env = StreamExecutionEnvironment.getExecutionEnvironment();
		}

		final int parallelism = parameters.getInt("parallelism", -1);
		if (parallelism > 0) {
			env.setParallelism(parallelism);
		}

		env.getConfig().setGlobalJobParameters(parameters);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
				Integer.MAX_VALUE,
				Time.of(15, TimeUnit.SECONDS) // delay
		));
		return env;
	}

	/**
	 * Checks whether the environment should be set up in local mode (with Web UI,...).
	 */
	public static boolean isLocal(ParameterTool parameters) {
		return parameters.getBoolean("local", false);
	}
}
