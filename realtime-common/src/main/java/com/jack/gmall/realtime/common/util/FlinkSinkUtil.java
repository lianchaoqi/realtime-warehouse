package com.jack.gmall.realtime.common.util;

import com.jack.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.util
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-14  20:02
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class FlinkSinkUtil {

    public static Sink<String> getKafkaSource(String groupId, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("jack-" + topic + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }
}
