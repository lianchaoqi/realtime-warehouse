package com.jack.gmall.realtime.dwd.db.split.app;

import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.dwd.db.split.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-18  22:12
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class T extends BaseApp {
    public static void main(String[] args) {

        new T().start(
                8888,
                4,
                "ttttt",
                Constant.TOPIC_LOG
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
