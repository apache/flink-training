package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable variant of {@link com.ververica.flink.training.provided.ExtendedMeasurement.Location}.
 */
@SuppressWarnings("WeakerAccess")
@TypeInfo(Location.LocationTypeInfoFactory.class)
public class Location {
	private final double longitude;
	private final double latitude;
	private final double height;

    /**
     * Constructor.
     */
	public Location(double longitude, double latitude, double height) {
		this.longitude = longitude;
		this.latitude = latitude;
		this.height = height;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getHeight() {
		return height;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Location location = (Location) o;
		return Double.compare(location.longitude, longitude) == 0 &&
				Double.compare(location.latitude, latitude) == 0 &&
				Double.compare(location.height, height) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(longitude, latitude, height);
	}

	public static class LocationTypeInfoFactory extends TypeInfoFactory<Location> {
		@Override
		public TypeInformation<Location> createTypeInfo(
				Type t,
				Map<String, TypeInformation<?>> genericParameters) {
			return LocationTypeInfo.INSTANCE;
		}
	}
}
