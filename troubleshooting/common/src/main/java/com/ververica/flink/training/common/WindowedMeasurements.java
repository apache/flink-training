package com.ververica.flink.training.common;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Objects;

@SuppressWarnings({"unused", "RedundantSuppression"})
@DoNotChangeThis
public class WindowedMeasurements {

	private long windowStart;
	private long windowEnd;
	private String location;
	private long eventsPerWindow;
	private double sumPerWindow;

	public WindowedMeasurements() {
	}

	public WindowedMeasurements(final long windowStart, final long windowEnd, final String location,
								final long eventsPerWindow, final double sumPerWindow) {
		this.windowStart = windowStart;
		this.windowEnd = windowEnd;
		this.location = location;
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

	public void setWindow(TimeWindow window) {
		setWindowStart(window.getStart());
		setWindowEnd(window.getEnd());
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(final String location) {
		this.location = location;
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

	public void addMeasurement(Measurement measurement) {
		addMeasurement(measurement.getValue());
	}

	public void addMeasurement(double value) {
		sumPerWindow += value;
		++eventsPerWindow;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final WindowedMeasurements that = (WindowedMeasurements) o;
		return windowStart == that.windowStart &&
				windowEnd == that.windowEnd &&
				eventsPerWindow == that.eventsPerWindow &&
				Double.compare(that.sumPerWindow, sumPerWindow) == 0 &&
				Objects.equals(location, that.location);
	}

	@Override
	public int hashCode() {
		return Objects.hash(windowStart, windowEnd, location, eventsPerWindow, sumPerWindow);
	}

	@Override
	public String toString() {
		return "WindowedMeasurements{" + "windowStart=" + windowStart +
				", windowEnd=" + windowEnd +
				", location='" + location + '\'' +
				", eventsPerWindow=" + eventsPerWindow +
				", sumPerWindow=" + sumPerWindow +
				'}';
	}
}
