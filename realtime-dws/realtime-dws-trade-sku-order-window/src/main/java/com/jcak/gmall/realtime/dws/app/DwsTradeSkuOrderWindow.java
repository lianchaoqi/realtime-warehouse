package com.jcak.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.DateFormatUtil;
import com.jack.gmall.realtime.common.util.HBaseUtil;
import com.jcak.gmall.realtime.dws.function.RedisGetRichMapfunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jcakgmall.realtime.dws.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-05-07  20:47
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(11010, 2, "DwdTradeSkuOrderWindow1", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(stream);

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = etlStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        })).keyBy((KeySelector<JSONObject, String>) value -> value.getString("id"));

        SingleOutputStreamOperator<TradeSkuOrderBean> addSumStream = sumExchange(jsonObjectStringKeyedStream);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = curWindowReduce(addSumStream);


//        普通关联
        SingleOutputStreamOperator<TradeSkuOrderBean> res = getDimInfo(reduceStream);
//        写出到doris
//        res.map(
//                new DorisMapFunction<>()
//        ).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));


    }


    /**
     * 度量值修正
     *
     * @param jsonObjectStringKeyedStream
     * @return
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> sumExchange(KeyedStream<JSONObject, String> jsonObjectStringKeyedStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> addSumStream = jsonObjectStringKeyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            private MapState<String, BigDecimal> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmount = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmount.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                mapState = getRuntimeContext().getMapState(lastAmount);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                BigDecimal originalAmount = mapState.get("originalAmount");
                BigDecimal activityReduceAmount = mapState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = mapState.get("couponReduceAmount");
                BigDecimal orderAmount = mapState.get("orderAmount");

                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;

                BigDecimal curOrignalAmount = jsonObject.getBigDecimal("order_price").multiply(jsonObject.getBigDecimal("sku_num"));

                mapState.put("originalAmount", curOrignalAmount);
                mapState.put("activityReduceAmount", jsonObject.getBigDecimal("split_activity_amount"));
                mapState.put("couponReduceAmount", jsonObject.getBigDecimal("split_coupon_amount"));
                mapState.put("orderAmount", orderAmount);
                collector.collect(TradeSkuOrderBean.builder().skuId(jsonObject.getString("sku_id")).orderDetailId(jsonObject.getString("id")).ts(jsonObject.getLong("ts")).originalAmount(curOrignalAmount.subtract(originalAmount)).orderAmount(jsonObject.getBigDecimal("split_total_amount").subtract(orderAmount)).activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount").subtract(activityReduceAmount)).couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount)).build());


            }
        });
        return addSumStream;
    }

    /**
     * 分组开窗聚合
     *
     * @param addSumStream
     * @return
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> curWindowReduce(SingleOutputStreamOperator<TradeSkuOrderBean> addSumStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = addSumStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
            @Override
            public String getKey(TradeSkuOrderBean value) throws Exception {
                return value.getSkuId();
            }
        }).window(
                //这里记得造数据的时候   数据的事件不在当前 所以不输出  ，这里用了处理时
                //正式需求记得用事件事件
                TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L))).reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                TimeWindow window = context.window();
                String startDt = DateFormatUtil.tsToDateTime(window.getStart());
                String endDt = DateFormatUtil.tsToDateTime(window.getEnd());
                String currentDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (TradeSkuOrderBean tradeSkuOrderBean : iterable) {
                    tradeSkuOrderBean.setStt(startDt);
                    tradeSkuOrderBean.setEdt(endDt);
                    tradeSkuOrderBean.setCurDate(currentDt);
                    collector.collect(tradeSkuOrderBean);
                }
            }
        });
        return reduceStream;
    }

    /**
     * //关联维度信息
     *
     * @param reduceStream
     * @return
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> getDimInfo(SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> res = reduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getConnection();

            }

            @Override
            public void close() throws Exception {
                HBaseUtil.close(connection);
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                //通过hbase api获取各种维度维度信息
                JSONObject dimSkuInfo = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", tradeSkuOrderBean.getSkuId());
                //通过hbase api获取各种维度维度信息
                JSONObject dimSpuInfo = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_spu_info", tradeSkuOrderBean.getSkuId());
                //通过hbase api获取各种维度维度信息
                JSONObject dimBaseCategory3 = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_base_category3", tradeSkuOrderBean.getSkuId());

                JSONObject dimBaseCategory2 = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_base_category2", tradeSkuOrderBean.getSkuId());

                JSONObject dimBaseCategory1 = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_base_category1", tradeSkuOrderBean.getSkuId());

                JSONObject dimBaseTrademark = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_base_trademark", tradeSkuOrderBean.getSkuId());

                //将维度信息与事实关联  补全信息
                // id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                tradeSkuOrderBean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                tradeSkuOrderBean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                tradeSkuOrderBean.setSpuId(dimSkuInfo.getString("spu_id"));
                tradeSkuOrderBean.setSkuName(dimSkuInfo.getString("sku_name"));
                tradeSkuOrderBean.setCategory3Name(dimBaseCategory3.getString("name"));
                tradeSkuOrderBean.setCategory2Id(dimBaseCategory3.getString("category2_id"));
                tradeSkuOrderBean.setCategory2Name(dimBaseCategory2.getString("name"));
                tradeSkuOrderBean.setCategory1Id(dimBaseCategory2.getString("category1_id"));
                tradeSkuOrderBean.setCategory1Name(dimBaseCategory1.getString("name"));
                tradeSkuOrderBean.setTrademarkName(dimBaseTrademark.getString("tm_name"));
                return tradeSkuOrderBean;
            }
        });
        return res;
    }

    private SingleOutputStreamOperator<JSONObject> getEtlStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    Long ts = jsonObject.getLong("ts");
                    String id = jsonObject.getString("id");
                    String skuId = jsonObject.getString("sku_id");
                    if (ts != null && id != null && skuId != null) {
                        jsonObject.put("ts", ts * 1000);
                        out.collect(jsonObject);
                    }
                }

            }
        });
    }
}
