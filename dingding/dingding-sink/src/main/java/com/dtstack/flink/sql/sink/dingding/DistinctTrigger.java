package com.dtstack.flink.sql.sink.dingding;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

public class DistinctTrigger extends Trigger<Row, TimeWindow> {

    private final ValueStateDescriptor<Integer> stateDesc;

    DistinctTrigger() {
        this.stateDesc = new ValueStateDescriptor<>("DistinctState", Types.INT());
    }

    @Override
    public TriggerResult onElement(Row row, long timestamp, TimeWindow timeWindow, TriggerContext ctx)
            throws Exception {
        ValueState<Integer> distinctState = ctx.getPartitionedState(this.stateDesc);

        Integer counter = distinctState.value();
        if (counter == null || counter == 0) {
            distinctState.update(1);
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            distinctState.update(++counter);
            return TriggerResult.PURGE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, TimeWindow timeWindow, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long timestamp, TimeWindow timeWindow, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext ctx) {
        ctx.getPartitionedState(this.stateDesc).clear();
    }

}