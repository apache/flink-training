package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.ververica.flink.training.provided.ExtendedMeasurement;
import com.ververica.flink.training.provided.GeoUtils2;
import com.ververica.flink.training.provided.MeanGauge;
import com.ververica.flink.training.provided.ObjectReuseExtendedMeasurementSource;
import com.ververica.flink.training.provided.WeatherUtils;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;

/**
 * Object reuse exercise.
 */
public class ObjectReuseJob {

	/**
	 * Creates and starts the object reuse job.
	 *
	 * @throws Exception if the application is misconfigured or fails during job submission
	 */
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

		final boolean objectReuse = parameters.getBoolean("objectReuse", false);
		if (objectReuse) {
			env.getConfig().enableObjectReuse();
		}

		//Checkpointing Configuration
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

		final OutputTag<ExtendedMeasurement> temperatureTag =
				new OutputTag<ExtendedMeasurement>("temperature") {
					private static final long serialVersionUID = -3127503822430851744L;
				};
		final OutputTag<ExtendedMeasurement> windTag =
				new OutputTag<ExtendedMeasurement>("wind") {
					private static final long serialVersionUID = 4249595595891069268L;
				};

		SingleOutputStreamOperator<ExtendedMeasurement> splitStream = env
				.addSource(new ObjectReuseExtendedMeasurementSource())
				.name("FakeMeasurementSource")
				.uid("FakeMeasurementSource")
				.keyBy(ExtendedMeasurement::getSensor)
				.process(new SplitSensors(temperatureTag, windTag))
				.name("SplitSensors")
				.uid("SplitSensors");

		// (1) stream with the temperature converted into local temperature units (°F in the US)
		splitStream.getSideOutput(temperatureTag)
				.map(new ConvertToLocalTemperature())
				.name("ConvertToLocalTemperature")
				.uid("ConvertToLocalTemperature")
				.addSink(new DiscardingSink<>())
				.name("LocalizedTemperatureSink")
				.uid("LocalizedTemperatureSink")
				.disableChaining();

		// no need to do keyBy again; we did not change the key!
		KeyedStream<ExtendedMeasurement, ExtendedMeasurement.Sensor> keyedTemperatureStream =
				DataStreamUtils.reinterpretAsKeyedStream(
						splitStream.getSideOutput(temperatureTag),
						ExtendedMeasurement::getSensor);

		// (2) stream with an (exponentially) moving average of the temperature (smoothens sensor
		//     measurements, variant A); then converted into local temperature units (°F in the US)
		keyedTemperatureStream
				.flatMap(new MovingAverageSensors())
				.name("MovingAverageTemperature")
				.uid("MovingAverageTemperature")
				.map(new ConvertToLocalTemperature())
				.name("ConvertToLocalAverageTemperature")
				.uid("ConvertToLocalAverageTemperature")
				.addSink(new DiscardingSink<>())
				.name("LocalizedAverageTemperatureSink")
				.uid("LocalizedAverageTemperatureSink")
				.disableChaining();

		// (3) stream with an windowed-average of the temperature (to smoothens sensor
		//     measurements, variant B); then converted into local temperature units (°F in the US)
		keyedTemperatureStream
				.timeWindow(Time.of(10, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
				.aggregate(new WindowAverageSensor())
				.name("WindowAverageTemperature")
				.uid("WindowAverageTemperature")
				.map(new ConvertToLocalTemperature())
				.name("ConvertToLocalWindowedTemperature")
				.uid("ConvertToLocalWindowedTemperature")
				.addSink(new DiscardingSink<>())
				.name("LocalizedWindowedTemperatureSink")
				.uid("LocalizedWindowedTemperatureSink")
				.disableChaining();

		// (4) stream with the wind speed converted into local speed units (mph in the US)
		splitStream.getSideOutput(windTag)
				.map(new ConvertToLocalWindSpeed())
				.name("NormalizeWindSpeed")
				.uid("NormalizeWindSpeed")
				.addSink(new DiscardingSink<>())
				.name("WindSink")
				.uid("WindSink")
				.disableChaining();

		env.execute(ObjectReuseJob.class.getSimpleName());
	}

	/**
	 * Splits a stream into multiple side-outputs, one for each sensor.
	 */
	private static class SplitSensors extends
			KeyedProcessFunction<ExtendedMeasurement.Sensor, ExtendedMeasurement, ExtendedMeasurement> {
		private static final long serialVersionUID = 1L;

		private final EnumMap<ExtendedMeasurement.SensorType, OutputTag<ExtendedMeasurement>>
				outputTagBySensor =
				new EnumMap<>(ExtendedMeasurement.SensorType.class);

		SplitSensors(
				OutputTag<ExtendedMeasurement> temperatureTag,
				OutputTag<ExtendedMeasurement> windTag) {
			outputTagBySensor.put(ExtendedMeasurement.SensorType.Temperature, temperatureTag);
			outputTagBySensor.put(ExtendedMeasurement.SensorType.Wind, windTag);
		}

		@Override
		public void processElement(
				ExtendedMeasurement value,
				Context ctx,
				Collector<ExtendedMeasurement> out) {
			ExtendedMeasurement.SensorType sensorType = value.getSensor().getSensorType();
			OutputTag<ExtendedMeasurement> output = outputTagBySensor.get(sensorType);
			ctx.output(output, value);
		}
	}

	/**
	 * Implements an exponentially moving average with a coefficient of <code>0.5</code>, i.e.
	 * <ul>
	 *     <li><code>avg[0] = value[0]</code> (not forwarded to the next stream)</li>
	 *     <li><code>avg[i] = avg[i-1] * 0.5 + value[i] * 0.5</code> (for <code>i > 0</code>)</li>
	 * </ul>
	 *
	 * <p>See <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">
	 *     https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average</a>
	 */
	private static class MovingAverageSensors extends
			RichFlatMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
		private static final long serialVersionUID = 1L;

		private final Map<ExtendedMeasurement.Sensor, Tuple2<Float, Double>> lastAverage =
				new HashMap<>();

		@Override
		public void flatMap(ExtendedMeasurement value, Collector<ExtendedMeasurement> out) {
			ExtendedMeasurement.Sensor sensor = value.getSensor();

			Tuple2<Float, Double> last = lastAverage.get(sensor);
			if (last != null) {
				float newAccuracy = (last.f0 + value.getMeasurement().getAccuracy()) / 2.0f;
				double newValue = (last.f1 + value.getMeasurement().getValue()) / 2.0;
				value.getMeasurement().setAccuracy(newAccuracy);
				value.getMeasurement().setValue(newValue);
				// do not forward the first value (it only stands alone)
				out.collect(value);
			}
			lastAverage.put(
					sensor,
					Tuple2.of(
							value.getMeasurement().getAccuracy(),
							value.getMeasurement().getValue()
					));
		}
	}

	@SuppressWarnings("WeakerAccess")
	public static class WindowedAggregate {
		public double sumValue = 0.0;
		public double sumAccuracy = 0.0;
		public long count = 0;
		public ExtendedMeasurement lastValue = null;

		/**
		 * Constructor.
		 */
		public WindowedAggregate() {
		}
	}

	/**
	 * Aggregate function determining average sensor values and accuracies per sensor instance.
	 */
	private static class WindowAverageSensor implements
			AggregateFunction<ExtendedMeasurement, WindowedAggregate, ExtendedMeasurement> {
		@Override
		public WindowedAggregate createAccumulator() {
			return new WindowedAggregate();
		}

		@Override
		public WindowedAggregate add(ExtendedMeasurement value, WindowedAggregate accumulator) {
			accumulator.sumAccuracy += value.getMeasurement().getAccuracy();
			accumulator.sumValue += value.getMeasurement().getValue();
			accumulator.count++;
			accumulator.lastValue = value;
			return accumulator;
		}

		@Override
		public ExtendedMeasurement getResult(WindowedAggregate accumulator) {
			ExtendedMeasurement result = accumulator.lastValue;
			result.getMeasurement().setValue(accumulator.sumValue / accumulator.count);
			result.getMeasurement()
					.setAccuracy((float) (accumulator.sumAccuracy / accumulator.count));
			return result;
		}

		@Override
		public WindowedAggregate merge(WindowedAggregate a, WindowedAggregate b) {
			a.count += b.count;
			a.sumValue += b.sumValue;
			a.sumAccuracy += b.sumAccuracy;
			if (b.lastValue.getMeasurement().getTimestamp() >
					a.lastValue.getMeasurement().getTimestamp()) {
				a.lastValue = b.lastValue;
			}
			return a;
		}
	}

	/**
	 * Converts SI units to locale-dependent units, i.e. °C to °F for the US. Adds a custom metric
	 * to report temperatures in the US.
	 */
	private static class ConvertToLocalTemperature extends
			RichMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
		private static final long serialVersionUID = 1L;

		private transient MeanGauge normalizedTemperatureUS;

		@Override
		public void open(final Configuration parameters) {
			normalizedTemperatureUS = getRuntimeContext().getMetricGroup()
					.gauge("normalizedTemperatureUSmean", new MeanGauge());
			getRuntimeContext().getMetricGroup().gauge(
					"normalizedTemperatureUSmin", new MeanGauge.MinGauge(normalizedTemperatureUS));
			getRuntimeContext().getMetricGroup().gauge(
					"normalizedTemperatureUSmax", new MeanGauge.MaxGauge(normalizedTemperatureUS));
		}

		@Override
		public ExtendedMeasurement map(ExtendedMeasurement value) {
			ExtendedMeasurement.Location location = value.getLocation();
			if (GeoUtils2.isInUS(location.getLongitude(), location.getLatitude())) {
				ExtendedMeasurement.MeasurementValue measurement = value.getMeasurement();
				double normalized = WeatherUtils.celciusToFahrenheit(measurement.getValue());
				measurement.setValue(normalized);
				normalizedTemperatureUS.addValue(normalized);
			}
			return value;
		}
	}

	/**
	 * Converts SI units to locale-dependent units, i.e. km/h to mph for the US. Adds a custom metric
	 * to report wind speeds in the US.
	 */
	private static class ConvertToLocalWindSpeed extends
			RichMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
		private static final long serialVersionUID = 1L;

		@Override
		public ExtendedMeasurement map(ExtendedMeasurement value) {
			ExtendedMeasurement.Location location = value.getLocation();
			if (GeoUtils2.isInUS(location.getLongitude(), location.getLatitude())) {
				ExtendedMeasurement.MeasurementValue measurement = value.getMeasurement();
				measurement.setValue(WeatherUtils.kphToMph(measurement.getValue()));
			}
			return value;
		}
	}
}
