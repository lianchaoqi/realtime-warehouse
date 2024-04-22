package com.jack.gmall.realtime.common.util;

import com.jack.gmall.realtime.common.constant.Constant;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.util
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-19  16:51
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class FlinkSQLUtil {

    //生成表with配置
    public static String getKafkaSourcePreStr(String topic, String groupId) {

        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaTopicDb(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` bigint,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>,\n" +
                "  `proc_time` as PROCTIME()\n" +
                ")" + getKafkaSourcePreStr(Constant.TOPIC_DB, groupId);
    }

    public static String getHBaseDimBaseDic() {
        return "CREATE TABLE dim_base_dic(\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_code INT,dic_name STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+Constant.HBASE_NAMESPACE+":dim_base_dic',\n" +
//                " 'table-name' = 'gmall2023:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '"+Constant.HBASE_ZOOKEEPER_QUORUM+"'\n" +
                ");";

    }
    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }


    public static String getUpsertKafkaDDL(String topic) {
        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.ignore-parse-errors' = 'true'," +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}