package com.SensorData.Functions;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * A simple {@link ProcessWindowFunction}, which wraps a count of {@link com.SensorData.SensorData}s into an
 * instance of {@link WindowedFootTraffic}.
 *
 **/

public class FootTrafficCollector extends ProcessWindowFunction<Long, WindowedFootTraffic, String, TimeWindow> {

    @Override
    public void process(
            final String visitor,
            final Context context,
            final Iterable<Long> elements,
            final Collector<WindowedFootTraffic> out) throws Exception {

        Long count = elements.iterator().next();
        out.collect(new WindowedFootTraffic(new Date(context.window().getStart()), new Date(context.window().getEnd()), visitor, count));
    }
}
