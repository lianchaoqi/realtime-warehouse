package com.jack.gmall.realtime.dwd.db.split.app;

import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.dwd.db.split.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-17  23:46
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(8889, 4, "DwdBaseLog", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {


    }
}
