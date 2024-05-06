package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.bean.TrafficPageView;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.DateFormatUtil;
import com.jack.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.atguigu.gmall.realtime.dws.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-24  23:15
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                11008,
                4,
                "DwsTrafficVcChArIsNewPageViewWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //过滤数据
        SingleOutputStreamOperator<TrafficPageView> pageViewSingleOutputStreamOperator = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String instr, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(instr);
                    Long ts = jsonObject.getLong("ts");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if (mid != null && ts != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("清洗数据" + instr);
                }
            }
        }).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        }).process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageView>() {
                    private ValueState<String> stringValueState;
                    private final ValueStateDescriptor<String> midValueStateDesc = new ValueStateDescriptor<>("mid_value_state", String.class);

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        //TODO 记得添加状态的存活时间
                        midValueStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        stringValueState = getRuntimeContext().getState(midValueStateDesc);

                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageView>.Context context, Collector<TrafficPageView> collector) throws Exception {
                        String eventDt = DateFormatUtil.tsToDate(jsonObject.getLong("ts"));
                        String lastDt = stringValueState.value();
                        JSONObject page = jsonObject.getJSONObject("page");
                        JSONObject common = jsonObject.getJSONObject("common");
                        Long ucCt = 0L;
                        Long svCt = 0L;
                        //判断是否是独立用户（今天新来的数据）
                        if (lastDt == null || !lastDt.equals(eventDt)) {
                            ucCt = 1L;
                            stringValueState.update(eventDt);
                        }
                        String lastpageid = page.getString("last_page_id");
                        //新会话
                        if (lastpageid == null) {
                            svCt = 1L;
                        }


                        collector.collect(
                                TrafficPageView
                                        .builder()
                                        .vc(common.getString("vc"))
                                        .ar(common.getString("ar"))
                                        .ch(common.getString("ch"))
                                        .ch(common.getString("is_new"))
                                        .uvCt(ucCt)
                                        .svCt(svCt)
                                        .pvCt(1L)
                                        .durSum(page.getLong("during_time"))
                                        .sid(common.getString("sid"))
                                        .ts(jsonObject.getLong("ts"))
                                        .build()
                        );


                    }
                }
        );

        //注册水位线
        WatermarkStrategy<TrafficPageView> trafficPageViewWatermarkStrategy = WatermarkStrategy
                .<TrafficPageView>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<TrafficPageView>() {
                            @Override
                            public long extractTimestamp(TrafficPageView trafficPageView, long l) {
                                return trafficPageView.getTs();
                            }
                        }

                );
        //开窗

        SingleOutputStreamOperator<String> res = pageViewSingleOutputStreamOperator
                .assignTimestampsAndWatermarks(trafficPageViewWatermarkStrategy)
                .keyBy(new KeySelector<TrafficPageView, String>() {
                    @Override
                    public String getKey(TrafficPageView trafficPageView) throws Exception {
                        return trafficPageView.getVc() + trafficPageView.getCh() + trafficPageView.getAr() + trafficPageView.getIsNew();
                    }
                }).window(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                ).reduce(

                        new AggregationFunction<TrafficPageView>() {
                            // 聚合
                            @Override
                            public TrafficPageView reduce(TrafficPageView trafficPageView, TrafficPageView t1) throws Exception {
                                trafficPageView.setSvCt(t1.getSvCt() + trafficPageView.getSvCt());
                                trafficPageView.setUvCt(t1.getUvCt() + trafficPageView.getUvCt());
                                trafficPageView.setDurSum(t1.getDurSum() + trafficPageView.getDurSum());
                                trafficPageView.setPvCt(t1.getPvCt() + trafficPageView.getPvCt());
                                return trafficPageView;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageView, TrafficPageView, String, TimeWindow>() {
                            @Override
                            public void process(String key
                                    , ProcessWindowFunction<TrafficPageView
                                    , TrafficPageView, String, TimeWindow>.Context context
                                    , Iterable<TrafficPageView> iterable
                                    , Collector<TrafficPageView> collector) throws Exception {

                                TimeWindow window = context.window();
                                String sst = DateFormatUtil.tsToDateTime(window.getStart());
                                String et = DateFormatUtil.tsToDateTime(window.getEnd());
                                String curt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                                for (TrafficPageView trafficPageView : iterable) {
                                    trafficPageView.setStt(sst);
                                    trafficPageView.setEdt(et);
                                    trafficPageView.setCur_date(curt);
                                    collector.collect(trafficPageView);
                                }
                            }
                        }
                ).map(new MapFunction<TrafficPageView, String>() {
                    @Override
                    public String map(TrafficPageView trafficPageView) throws Exception {
                        SerializeConfig serializeConfig = new SerializeConfig();
                        serializeConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                        return JSONObject.toJSONString(trafficPageView, serializeConfig);
                    }
                });
        //写出
        res.
                sinkTo(
                        FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));
    }
}
