package com.dtstack.flink.sql.sink.dingding;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.dingding.table.DingdingSinkTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DingdingSink implements RetractStreamTableSink<Row>, IStreamSinkGener<DingdingSink>, Serializable {

    private DingdingSinkTableInfo dingdingSinkTableInfo;

    protected String[] fieldNames;

    private TypeInformation[] fieldTypes;

    private List<Integer> collectIndex;

    private TumblingProcessingTimeWindows window;

    @Override
    public DingdingSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        this.dingdingSinkTableInfo = (DingdingSinkTableInfo) targetTableInfo;
        this.window = TumblingProcessingTimeWindows.of(Time.seconds(Long.valueOf(dingdingSinkTableInfo.getTimeInternal())));

        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        DataStream<Row> dt = dataStream.filter((FilterFunction<Tuple2<Boolean, Row>>) booleanRowTuple2 -> booleanRowTuple2.f0).process(new ProcessFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public void processElement(Tuple2<Boolean, Row> booleanRowTuple2, Context context, Collector<Row> collector) throws Exception {
                System.out.println("row ==============" + booleanRowTuple2);
                System.out.println("dingdingSinkTableInfo =================== {}" + dingdingSinkTableInfo + "  field  " + dingdingSinkTableInfo.getFieldList() + " list "+ dingdingSinkTableInfo.getFields());
                collector.collect(booleanRowTuple2.f1);
            }
        });
        DataStream<Row> disDataStream = dt.keyBy(new DistinctKeySelector(collectIndex)).window(window).trigger(new DistinctTrigger()).process(new DistinctProcessWindowFunction());
        disDataStream.addSink(new SinkFunction(dingdingSinkTableInfo)).setParallelism(1);
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        this.fieldNames = strings;
        this.fieldTypes = typeInformations;
        List<String> list = Arrays.asList(strings);
        if (dingdingSinkTableInfo.getDistincts() != null){
            List<String> list1 = Arrays.asList(dingdingSinkTableInfo.getDistincts());
            collectIndex = list1.stream().map(list::indexOf).collect(Collectors.toList());
        }
        return this;
    }

    private static class SinkFunction extends RichSinkFunction<Row> {
        private DingdingService dingdingService;
        private DingdingSinkTableInfo dingdingSinkTableInfo;

        public SinkFunction(DingdingSinkTableInfo dingdingSinkTableInfo) {
            this.dingdingService = new DingdingService();
            this.dingdingSinkTableInfo = dingdingSinkTableInfo;
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            System.out.println("vale =========" + value);
            dingdingService.emit(dingdingSinkTableInfo, value);
        }
    }

    private static class DistinctKeySelector implements KeySelector<Row, String> {

        private List<Integer> distinctKeyIndexes;

        DistinctKeySelector(List<Integer> distinctKeyIndexes) {
            this.distinctKeyIndexes = distinctKeyIndexes;
        }

        @Override
        public String getKey(Row row) {
            StringBuilder sb = new StringBuilder();
            if (distinctKeyIndexes == null){
                sb.append(row.toString());
            }else {
                for (Integer index : distinctKeyIndexes) {
                    sb.append(row.getField(index));
                }
            }

            return sb.toString();
        }
    }
}
