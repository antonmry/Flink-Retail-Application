package com.SensorData.Functions;

import com.SensorData.SensorData;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This function is responsible for Counting the number of visitors for FootTraffic
 *
 */

public class Counting implements AggregateFunction< SensorData, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(final SensorData value, final Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(final Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(final Long a, final Long b) {
        return a + b;
    }
}