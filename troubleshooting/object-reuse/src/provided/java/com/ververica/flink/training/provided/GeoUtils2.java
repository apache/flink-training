package com.ververica.flink.training.provided;

import java.util.SplittableRandom;

/**
 * GeoUtils provides utility methods to deal with locations for the data streaming exercises.
 */
public class GeoUtils2 {

	// bounding box of the area of the USA
	public static final double US_LON_EAST = -66.9326;
	public static final double US_LON_WEST = -125.0011;
	public static final double US_LAT_NORTH = 49.5904;
	public static final double US_LAT_SOUTH =  24.9493;

	/**
	 * Checks if a location specified by longitude and latitude values is
	 * within the geo boundaries of the USA.
	 *
	 * @param lon longitude of the location to check
	 * @param lat latitude of the location to check
	 *
	 * @return true if the location is within US boundaries, otherwise false.
	 */
	public static boolean isInUS(double lon, double lat) {
		return !(lon > US_LON_EAST || lon < US_LON_WEST) &&
				!(lat > US_LAT_NORTH || lat < US_LAT_SOUTH);
	}

	/**
	 * Get a random longitude that is within the US bounding box.
	 */
	public static double getRandomLongitudeUS(SplittableRandom rnd) {
		return rnd.nextDouble() * (US_LON_EAST - US_LON_WEST) +
				US_LON_WEST;
	}

	/**
	 * Get a random latitude that is within the US bounding box.
	 */
	public static double getRandomLatitudeUS(SplittableRandom rnd) {
		return rnd.nextDouble() * (US_LAT_NORTH - US_LAT_SOUTH) +
				US_LAT_SOUTH;
	}

	// bounding box of the area of the USA
	public static final double DE_LON_EAST = 15.0419319;
	public static final double DE_LON_WEST = 5.8663153;
	public static final double DE_LAT_NORTH = 55.099161;
	public static final double DE_LAT_SOUTH =  47.2701114;

	/**
	 * Checks if a location specified by longitude and latitude values is
	 * within the geo boundaries of Germany.
	 *
	 * @param lon longitude of the location to check
	 * @param lat latitude of the location to check
	 *
	 * @return true if the location is within German boundaries, otherwise false.
	 */
	public static boolean isInDE(double lon, double lat) {
		return !(lon > DE_LON_EAST || lon < DE_LON_WEST) &&
				!(lat > DE_LAT_NORTH || lat < DE_LAT_SOUTH);
	}

	/**
	 * Get a random longitude that is within the DE bounding box.
	 */
	public static double getRandomLongitudeDE(SplittableRandom rnd) {
		return rnd.nextDouble() * (DE_LON_EAST - DE_LON_WEST) +
				DE_LON_WEST;
	}

	/**
	 * Get a random latitude that is within the DE bounding box.
	 */
	public static double getRandomLatitudeDE(SplittableRandom rnd) {
		return rnd.nextDouble() * (DE_LAT_NORTH - DE_LAT_SOUTH) +
				DE_LAT_SOUTH;
	}
}
