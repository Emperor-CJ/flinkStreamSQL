package com.dtstack.flink.sql.sink.dingding;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DistinctProcessWindowFunction extends ProcessWindowFunction<Row, Row, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Row> iterable, Collector<Row> out) {
        for (Row anIterable : iterable) {
            out.collect(anIterable);
        }
    }
}