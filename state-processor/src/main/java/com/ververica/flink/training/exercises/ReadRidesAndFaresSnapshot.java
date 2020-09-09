package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.util.Collector;

/**
 * Java implementation for an example using the State Processor API to read and display
 * the contents of a retained checkpoint or savepoint from RidesAndFaresSolution.
 *
 * <p>Required parameter:
 *
 * <p>--input path-to-snapshot
 *
 * <p>e.g., --input file:///tmp/checkpoints/3bb27ec3cedb40d19ff31c4617e54715/chk-5
 */
public class ReadRidesAndFaresSnapshot {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
		MemoryStateBackend backend = new MemoryStateBackend();

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		ExistingSavepoint sp = Savepoint.load(bEnv, input, backend);

		// the uid here must match the uid used in RidesAndFaresSolution
		DataSet<Tuple2<TaxiRide, TaxiFare>> keyedState = sp.readKeyedState("enrichment", new ReadRidesAndFares());

		keyedState.print();
	}

	static class ReadRidesAndFares extends KeyedStateReaderFunction<Long, Tuple2<TaxiRide, TaxiFare>> {
		ValueState<TaxiRide> ride;
		ValueState<TaxiFare> fare;

		@Override
		public void open(Configuration parameters) {

			// these state descriptors must be compatible with those used in RidesAndFaresSolution
			ride = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fare = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void readKey(
				Long key,
				Context context,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			out.collect(new Tuple2<>(ride.value(), fare.value()));
		}
	}
}
