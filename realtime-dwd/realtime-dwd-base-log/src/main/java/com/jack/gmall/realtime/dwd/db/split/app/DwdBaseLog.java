package com.jack.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author qilianchao@gyyx.cn
 * @date 2024-04-18
 * @Description
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(
                8888,
                4,
                "DwdBaseLog",
                Constant.TOPIC_LOG
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1：清洗掉空数据
        SingleOutputStreamOperator<JSONObject> etlDataStream = etlData(stream);
        //这是水位线key分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(etlDataStream);
        //键控流
        SingleOutputStreamOperator<JSONObject> processMain = is_new(keyedStream);
        OutputTag<String> errTag = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionsTag = new OutputTag<>("actions", TypeInformation.of(String.class));
        //拆分日志流
        SingleOutputStreamOperator<String> pageStreamp = splitLog(errTag
                , startTag, displayTag, actionsTag, processMain);

        //TODO 输出到kafka
        writeToKafka(pageStreamp);

    }

    private void writeToKafka(SingleOutputStreamOperator<String> pageStreamp) {
    }


    public SingleOutputStreamOperator<String> splitLog(
            OutputTag<String> errTag
            , OutputTag<String> startTag
            , OutputTag<String> displayTag
            , OutputTag<String> actionsTag
            , SingleOutputStreamOperator<JSONObject> streamSource) {
        return streamSource.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObjectValue, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        //核心处理逻辑，不同日志类型拆分不同的日志
                        JSONObject err = jsonObjectValue.getJSONObject("err");
                        if (err != null) {
                            context.output(errTag, err.toString());
                            //已经处理了 删除
                            jsonObjectValue.remove("err");
                        }
                        JSONObject page = jsonObjectValue.getJSONObject("page");
                        JSONObject start = jsonObjectValue.getJSONObject("start");
                        JSONObject common = jsonObjectValue.getJSONObject("common");
                        Long ts = jsonObjectValue.getLong("ts");

                        if (start != null) {
                            context.output(startTag, jsonObjectValue.toString());
                        } else if (page != null) {
                            //页面日志
                            JSONArray display = jsonObjectValue.getJSONArray("display");
                            if (display != null) {
                                for (int i = 0; i < display.size(); i++) {
                                    JSONObject jsonObject = display.getJSONObject(i);
                                    //添加公共信息
                                    jsonObject.put("common", common);
                                    jsonObject.put("ts", ts);
                                    jsonObject.put("page", page);
                                    context.output(displayTag, display.toString());
                                }
                            }
                            jsonObjectValue.remove("display");

                            JSONArray actions = jsonObjectValue.getJSONArray("actions");
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject jsonObject = actions.getJSONObject(i);
                                    //添加公共信息
                                    jsonObject.put("common", common);
                                    jsonObject.put("ts", ts);
                                    jsonObject.put("page", page);
                                    context.output(actionsTag, actions.toString());
                                }
                            }
                            jsonObjectValue.remove("actions");
                            //只保留page信息写出
                            collector.collect(jsonObjectValue.toString());
                        } else {
                            //留空
                        }
                    }
                }
        );
    }

    public SingleOutputStreamOperator<JSONObject> is_new(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("key-state", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        collector.collect(jsonObject);
                    }
                }
        );
    }

    public KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> etlDataStream) {
        return etlDataStream
                .assignTimestampsAndWatermarks(
                        //2：注册水位线
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                })
                ).keyBy(
                        new KeySelector<JSONObject, String>() {
                            @Override
                            public String getKey(JSONObject jsonObject) throws Exception {
                                //这里keyby的时候需要保障数据不为空，为空的话会报错
                                return jsonObject.getJSONObject("common").getString("mid");
                            }
                        }
                );
    }

    public SingleOutputStreamOperator<JSONObject> etlData(DataStreamSource<String> stream) {
        return stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {

                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            JSONObject page = jsonObject.getJSONObject("page");
                            JSONObject start = jsonObject.getJSONObject("start");
                            JSONObject common = jsonObject.getJSONObject("common");
                            if (page != null || start != null) {
                                if (common != null && jsonObject.getLong("ts") != null && common.getString("mid") != null) {

                                    collector.collect(jsonObject);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
    }
}
