package com.jack.gmall.realtime.dwd.db.split.app;

import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.dwd.db.split.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-18  22:12
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class T {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` bigint,\n" +
                "  `data` map<string,string>,\n" +
                "  `prco_time` as  PROCTIME(),\n" +
                "  `old` map<string,string>\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',\n" +
                "  'properties.group.id' = 'test01',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        TableResult tableResult = tableEnv.executeSql("select * from KafkaTable where `database`='gmall2023' and `table`='cart_info'");
        tableResult.print();


    }
}
