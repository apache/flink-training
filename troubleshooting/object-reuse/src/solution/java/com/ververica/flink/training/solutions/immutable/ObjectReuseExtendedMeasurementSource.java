package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.flink.training.provided.GeoUtils2;
import com.ververica.flink.training.provided.MeanGauge;

import java.util.SplittableRandom;

/**
 * Artificial source for sensor measurements (temperature and wind speed) of a pre-defined set of
 * sensors (per parallel instance) creating measurements for two locations (inside the bounding
 * boxes of Germany (DE) and the USA (US)) in SI units (°C and km/h).
 */
@SuppressWarnings("WeakerAccess")
public class ObjectReuseExtendedMeasurementSource extends
		RichParallelSourceFunction<ExtendedMeasurement> {

	private static final long serialVersionUID = 1L;

	private static final int NUM_SENSORS = 10_000;

	public static final int LOWER_TEMPERATURE_CELCIUS = -10;
	public static final int UPPER_TEMPERATURE_CELCIUS = 35;
	public static final int LOWER_WIND_SPEED_KMH = 0;
	public static final int UPPER_WIND_SPEED_KMH = 335;

	private volatile boolean running = true;

	private transient Sensor[] sensors;
	private transient Location[] locations;
	private transient double[] lastValue;
	private transient MeanGauge sourceTemperatureUS;

	@Override
	public void open(final Configuration parameters) {
		initSensors();

		sourceTemperatureUS = getRuntimeContext().getMetricGroup()
				.gauge("sourceTemperatureUSmean", new MeanGauge());
		getRuntimeContext().getMetricGroup().gauge(
				"sourceTemperatureUSmin", new MeanGauge.MinGauge(sourceTemperatureUS));
		getRuntimeContext().getMetricGroup().gauge(
				"sourceTemperatureUSmax", new MeanGauge.MaxGauge(sourceTemperatureUS));
	}

	@Override
	public void run(SourceContext<ExtendedMeasurement> ctx) {
		final SplittableRandom rnd = new SplittableRandom();
		final Object lock = ctx.getCheckpointLock();

		while (running) {
			ExtendedMeasurement event = randomEvent(rnd);

			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (lock) {
				ctx.collect(event);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	/**
	 * Creates sensor metadata that this source instance will work with.
	 */
	private void initSensors() {
		final SplittableRandom rnd = new SplittableRandom();
		final Sensor.SensorType[] sensorTypes =
				Sensor.SensorType.values();

		final int start = getRuntimeContext().getIndexOfThisSubtask() * NUM_SENSORS;
		this.sensors = new Sensor[NUM_SENSORS];
		this.lastValue = new double[NUM_SENSORS];
		this.locations = new Location[NUM_SENSORS];
		for (int i = 0; i < NUM_SENSORS; ++i) {
			long sensorId = start + i;
			long vendorId = sensorId % 100;
			final Sensor.SensorType sensorType =
					sensorTypes[(i / 2) % sensorTypes.length];
			sensors[i] = new Sensor(sensorId, vendorId, sensorType);

			lastValue[i] = randomInitialMeasurementValue(rnd, sensorType);

			// assume that a sensor has a fixed position
			locations[i] = randomInitialLocation(rnd, i);
		}
	}

	/**
	 * Creates a random measurement value that a sensor will start with.
	 */
	private double randomInitialMeasurementValue(
			SplittableRandom rnd,
			Sensor.SensorType sensorType) {
		switch (sensorType) {
			case Temperature:
				// -10°C - 35°C
				return rnd.nextInt(
						(UPPER_TEMPERATURE_CELCIUS - LOWER_TEMPERATURE_CELCIUS) * 10) / 10.0 +
						LOWER_TEMPERATURE_CELCIUS;
			case Wind:
				// 0km/h - 335km/h
				return rnd.nextInt((UPPER_WIND_SPEED_KMH - LOWER_WIND_SPEED_KMH) * 10) / 10.0 +
						LOWER_WIND_SPEED_KMH;
			default:
				throw new IllegalStateException("Unknown sensor type: " + sensorType);
		}
	}

	/**
	 * Creates a random location for a sensor, distinguishing two bounding boxes: US and DE.
	 */
	private static Location randomInitialLocation(SplittableRandom rnd, int i) {
		final double longitude;
		final double latitude;
		// let's assume that no selected region wraps around LON -180/+180
		if (i < NUM_SENSORS / 2) {
			// in US
			longitude = GeoUtils2.getRandomLongitudeUS(rnd);
			latitude = GeoUtils2.getRandomLatitudeUS(rnd);
		} else {
			// in DE
			longitude = GeoUtils2.getRandomLongitudeDE(rnd);
			latitude = GeoUtils2.getRandomLatitudeDE(rnd);
		}
		double height = rnd.nextDouble() * 3000;
		return new Location(longitude, latitude, height);
	}

	/**
	 * Creates a randomized sensor value during runtime of the source. Each new value differs
	 * slightly from the previous value that this sensor had.
	 */
	private ExtendedMeasurement randomEvent(SplittableRandom rnd) {
		int randomIdx = rnd.nextInt(sensors.length);
		Sensor sensor = sensors[randomIdx];
		Location location = locations[randomIdx];

		long timestamp = System.currentTimeMillis();

		final double value = randomChangeMeasurementValue(
				rnd,
				sensor.getSensorType(),
				location,
				lastValue[randomIdx]);

		lastValue[randomIdx] = value;

		final MeasurementValue measurement =
				new MeasurementValue(
						value,
						(float) (rnd.nextInt(100) - 50) / 10.0f, // +- 5
						timestamp);

		return new ExtendedMeasurement(
				new Sensor(
						sensor.getSensorId(), sensor.getVendorId(), sensor.getSensorType()),
				new Location(
						location.getLongitude(), location.getLatitude(), location.getHeight()),
				measurement);
	}

	/**
	 * Generates a new sensor value that is +-3 of the old value and reports a custom metric for
	 * sensor values in the US.
	 */
	private double randomChangeMeasurementValue(
			SplittableRandom rnd,
			Sensor.SensorType sensorType,
			Location location,
			double lastValue) {
		double change = rnd.nextDouble(6) - 3.0; // +- 3
		final double value;
		switch (sensorType) {
			case Temperature:
				value = newValueWithinBounds(
						lastValue, change, LOWER_TEMPERATURE_CELCIUS, UPPER_TEMPERATURE_CELCIUS);
				if (GeoUtils2.isInUS(location.getLongitude(), location.getLatitude())) {
					sourceTemperatureUS.addValue(value);
				}
				break;
			case Wind:
				value = newValueWithinBounds(
						lastValue, change, LOWER_WIND_SPEED_KMH, UPPER_WIND_SPEED_KMH);
				break;
			default:
				throw new InternalError("Unknown sensor type: " + sensorType);
		}
		return value;
	}

	/**
	 * Returns either <code>lastValue + change</code> (if within the given bounds) or
	 * <code>lastValue - change</code> (otherwise).
	 */
	private static double newValueWithinBounds(
			double lastValue,
			double change,
			double lowerLimit,
			double upperLimit) {
		double value;
		if (lastValue + change >= lowerLimit && lastValue + change <= upperLimit) {
			value = lastValue + change;
		} else {
			value = lastValue - change;
		}
		return value;
	}
}
