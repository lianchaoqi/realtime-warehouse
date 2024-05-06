package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.doris.shaded.org.apache.arrow.vector.complex.reader.DurationReader;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.atguigu.gmall.realtime.dws.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-29  16:46
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                11009
                , 4
                , Constant.DWS_USER_USER_LOGIN_WINDOW
                , Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            JSONObject common = jsonObject.getJSONObject("common");
                            JSONObject page = jsonObject.getJSONObject("page");
                            String uid = common.getString("uid");
                            String last_page_id = page.getString("last_page_id");
                            Long ts = jsonObject.getLong("ts");
                            if (uid != null && (last_page_id == null || "login".equals(last_page_id)) && ts != null) {
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("清洗脏数据: " + value);
                        }
                    }
                }
        );
        //加上水位线，自动将数据调为有序的状态
        SingleOutputStreamOperator<JSONObject> andWatermarks = jsonStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5L)
                ).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override

                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })
        );

    }
}
