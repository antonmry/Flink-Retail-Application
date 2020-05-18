/* This function was based on the "ClickEventStatistics Sink" mentioned in :
https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/docker-playgrounds/flink-operations-playground.html */

package com.SensorData.Functions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import java.util.Date;
import java.util.Objects;

/**
 * A Class for windowed Visitors count, it will be used in kafka Sink to receive the records,encodes them as JSON strings, and writes those back to a Kafka topic output.
 *
 */

public class WindowedFootTraffic {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss:SSS")
    private Date windowEnd;
    private String sensor_UID;
    private long count;

    public WindowedFootTraffic() {
    }

    public WindowedFootTraffic(
            final Date windowStart,
            final Date windowEnd,
            final String sensor_UID,
            final long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.sensor_UID = sensor_UID;
        this.count = count;
    }

    public Date getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(final Date windowStart) {
        this.windowStart = windowStart;
    }

    public Date getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(final Date windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getsensor_UID() {
        return sensor_UID;
    }

    public void setsensor_UID(final String sensor_UID) {
        this.sensor_UID = sensor_UID;
    }

    public long getCount() {
        return count;
    }

    public void setCount(final long count) {
        this.count = count;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final WindowedFootTraffic that = (WindowedFootTraffic) o;
        return count == that.count &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd) &&
                Objects.equals(sensor_UID, that.sensor_UID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, sensor_UID, count);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WindowedFootTraffic {");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd);
        sb.append(", sensor_UID='").append(sensor_UID).append('\'');
        sb.append(", numVisitors=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
