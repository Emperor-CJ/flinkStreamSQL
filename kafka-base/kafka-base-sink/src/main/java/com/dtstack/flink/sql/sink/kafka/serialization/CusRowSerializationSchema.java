package com.dtstack.flink.sql.sink.kafka.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

import java.nio.charset.Charset;

public class CusRowSerializationSchema implements SerializationSchema<CRow> {
    @Override
    public byte[] serialize(CRow cRow) {
        StringBuffer sb = new StringBuffer();
        Row row = cRow.row();
        for (int i = 0; i < row.getArity(); i++) {
            sb.append(row.getField(i));
            if (i != row.getArity() -1){
                sb.append(",");
            }
        }
        return sb.toString().getBytes(Charset.forName("UTF-8"));
    }
}
