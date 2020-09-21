/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import com.dtstack.flink.sql.sink.kafka.serialization.AvroCRowSerializationSchema;
import com.dtstack.flink.sql.sink.kafka.serialization.CsvCRowSerializationSchema;
import com.dtstack.flink.sql.sink.kafka.serialization.CusRowSerializationSchema;
import com.dtstack.flink.sql.sink.kafka.serialization.JsonCRowSerializationSchema;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.runtime.types.CRow;

import java.util.Optional;
import java.util.Properties;

/**
 * 抽象的kafka producer 的工厂类
 * 包括序统一的序列化工具的构造
 * company: www.dtstack.com
 * @author: toutian
 * create: 2019/12/26
 */
public abstract class AbstractKafkaProducerFactory {

    /**
     *  获取具体的KafkaProducer
     * eg create KafkaProducer010
     * @param kafkaSinkTableInfo
     * @param typeInformation
     * @param properties
     * @param partitioner
     * @return
     */
    public abstract RichSinkFunction<CRow> createKafkaProducer(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation, Properties properties, Optional<FlinkKafkaPartitioner<CRow>> partitioner, String[] partitionKeys);

    protected SerializationMetricWrapper createSerializationMetricWrapper(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation) {
        SerializationSchema<CRow> serializationSchema = createSerializationSchema(kafkaSinkTableInfo, typeInformation);
        return new SerializationMetricWrapper(serializationSchema);
    }

    private SerializationSchema<CRow> createSerializationSchema(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation) {
        SerializationSchema<CRow> serializationSchema = null;
        if (FormatType.JSON.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isNotBlank(kafkaSinkTableInfo.getSchemaString())) {
                serializationSchema = new JsonCRowSerializationSchema(kafkaSinkTableInfo.getSchemaString(), kafkaSinkTableInfo.getUpdateMode());
            } else if (typeInformation != null && typeInformation.getArity() != 0) {
                serializationSchema = new JsonCRowSerializationSchema(typeInformation, kafkaSinkTableInfo.getUpdateMode());
            } else {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }
        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isBlank(kafkaSinkTableInfo.getFieldDelimiter())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.CSV.name() + " must set fieldDelimiter");
            }
            final CsvCRowSerializationSchema.Builder serSchemaBuilder = new CsvCRowSerializationSchema.Builder(typeInformation);
            serSchemaBuilder.setFieldDelimiter(kafkaSinkTableInfo.getFieldDelimiter().toCharArray()[0]);
            serSchemaBuilder.setUpdateMode(kafkaSinkTableInfo.getUpdateMode());
            serializationSchema = serSchemaBuilder.build();
        } else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isBlank(kafkaSinkTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.AVRO.name() + " must set schemaString");
            }
            serializationSchema = new AvroCRowSerializationSchema(kafkaSinkTableInfo.getSchemaString(), kafkaSinkTableInfo.getUpdateMode());
        }else {
            serializationSchema = new CusRowSerializationSchema();
        }

        if (null == serializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSinkTableInfo.getSinkDataType());
        }

        return serializationSchema;
    }

}
