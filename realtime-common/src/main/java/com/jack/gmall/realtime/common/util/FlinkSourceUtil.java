package com.jack.gmall.realtime.common.util;

import com.jack.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

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
public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        KafkaSource<String> build = null;
        try {
            build = KafkaSource.<String>builder()
                    .setBootstrapServers(Constant.KAFKA_BROKERS)
                    .setGroupId(groupId)
                    .setTopics(topic)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                        @Override
                        public String deserialize(byte[] message) throws IOException {
                            if (message != null) {
                                return new String(message, StandardCharsets.UTF_8);
                            }
                            return null;
                        }

                        @Override
                        public boolean isEndOfStream(String nextElement) {
                            return false;
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return Types.STRING;
                        }
                    })
                    .build();
        } catch (Exception e) {
            System.out.println(e);
        }

        return build;
    }

    public static MySqlSource<String> getMySQLSource(String dbname, String tabName) {
        return MySqlSource
                .<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .port(Constant.MYSQL_PORT)
                //初始化读取
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .databaseList(dbname)
                .tableList(dbname + "." + tabName)
                .build();
    }
}
