package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;

/**
 * Various tools to convert units used in weather sensors.
 */
@DoNotChangeThis
public class WeatherUtils {

	/**
	 * Converts the given temperature from Fahrenheit to Celcius.
	 */
	public static double fahrenheitToCelcius(double temperatureInFahrenheit) {
		return ((temperatureInFahrenheit - 32) * 5.0) / 9.0;
	}

	/**
	 * Converts the given temperature from Celcius to Fahrenheit.
	 */
	public static double celciusToFahrenheit(double celcius) {
		return (celcius * 9.0) / 5.0 + 32;
	}

	/**
	 * Miles per hour -> kilometres per hour.
	 */
	public static double mphToKph(double mph) {
		return mph * 1.60934;
	}

	/**
	 * Kilometres per hour -> miles per hour.
	 */
	public static double kphToMph(double kph) {
		return kph / 1.60934;
	}
}
