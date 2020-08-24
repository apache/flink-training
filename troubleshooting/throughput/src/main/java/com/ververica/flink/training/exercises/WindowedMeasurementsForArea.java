package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.WindowedMeasurements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({"unused", "unchecked", "rawtypes"})
public class WindowedMeasurementsForArea {

	private long windowStart;
	private long windowEnd;
	private String area;
	private final List locations = new ArrayList();
	private long eventsPerWindow;
	private double sumPerWindow;

	public WindowedMeasurementsForArea() {
	}

	public WindowedMeasurementsForArea(
			final long windowStart,
			final long windowEnd,
			final String area,
			final String location,
			final long eventsPerWindow,
			final double sumPerWindow) {
		this.windowStart = windowStart;
		this.windowEnd = windowEnd;
		this.area = area;
		this.locations.add(location);
		this.eventsPerWindow = eventsPerWindow;
		this.sumPerWindow = sumPerWindow;
	}

	public long getWindowStart() {
		return windowStart;
	}

	public void setWindowStart(final long windowStart) {
		this.windowStart = windowStart;
	}

	public long getWindowEnd() {
		return windowEnd;
	}

	public void setWindowEnd(final long windowEnd) {
		this.windowEnd = windowEnd;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public List<String> getLocations() {
		return locations;
	}

	public void addLocation(final String location) {
		this.locations.add(location);
	}

	public void addAllLocations(final Collection<? extends String> locations) {
		this.locations.addAll(locations);
	}

	public static String getArea(String location) {
		if (location.length() > 0) {
			return location.substring(0, 1);
		} else {
			return "";
		}
	}

	public long getEventsPerWindow() {
		return eventsPerWindow;
	}

	public void setEventsPerWindow(final long eventsPerWindow) {
		this.eventsPerWindow = eventsPerWindow;
	}

	public double getSumPerWindow() {
		return sumPerWindow;
	}

	public void setSumPerWindow(final double sumPerWindow) {
		this.sumPerWindow = sumPerWindow;
	}

	public void addMeasurement(WindowedMeasurements measurements) {
		sumPerWindow += measurements.getSumPerWindow();
		eventsPerWindow += measurements.getEventsPerWindow();
		locations.add(measurements.getLocation());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WindowedMeasurementsForArea that = (WindowedMeasurementsForArea) o;
		return windowStart == that.windowStart &&
				windowEnd == that.windowEnd &&
				eventsPerWindow == that.eventsPerWindow &&
				Double.compare(that.sumPerWindow, sumPerWindow) == 0 &&
				Objects.equals(area, that.area) &&
				locations.equals(that.locations);
	}

	@Override
	public int hashCode() {
		return Objects.hash(windowStart, windowEnd, area, locations, eventsPerWindow, sumPerWindow);
	}

	@Override
	public String toString() {
		return "WindowedMeasurementsForArea{" +
				"windowStart=" + windowStart +
				", windowEnd=" + windowEnd +
				", area='" + area + '\'' +
				", locations=" + locations +
				", eventsPerWindow=" + eventsPerWindow +
				", sumPerWindow=" + sumPerWindow +
				'}';
	}
}
