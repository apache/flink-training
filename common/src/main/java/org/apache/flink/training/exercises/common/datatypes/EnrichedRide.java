package org.apache.flink.training.exercises.common.datatypes;

import org.apache.flink.training.exercises.common.utils.GeoUtils;

public class EnrichedRide extends TaxiRide {
    public int startCell;
    public int endCell;

    public EnrichedRide() {

    }

    public EnrichedRide(TaxiRide taxiRide) {
        super(taxiRide.rideId, taxiRide.isStart);
        this.startCell = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
        this.endCell = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EnrichedRide that = (EnrichedRide) o;

        if (startCell != that.startCell) return false;
        return endCell == that.endCell;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + startCell;
        result = 31 * result + endCell;
        return result;
    }

    @Override
    public String toString() {
        return "EnrichedRide{" +
                "taxiRide" + super.toString() +
                ", startCell=" + startCell +
                ", endCell=" + endCell +
                '}';
    }
}
