package com.jack.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.bean.TableProcessDim;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.function.ConnectBroadcastProcessFunctionImpl;
import com.jack.gmall.realtime.common.function.HBaseCreateDimRichFunction;
import com.jack.gmall.realtime.common.function.HBaseDataInsertFunction;
import com.jack.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.dim.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-15  09:27
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DimApp extends BaseApp {

    public static void main(String[] args) {
        new DimApp().start(11001, 4, "DimApp", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //获取kafka的维度表业务数据
        SingleOutputStreamOperator<JSONObject> kafkaDimData = getKafkaDimData(stream);
        kafkaDimData.print();
        //获取维度表的数据
        DataStreamSource<String> table_process_dim_source = env.fromSource(FlinkSourceUtil.getMySQLSource(
                        Constant.MYSQL_DIM_CONGFIG_DB
                        , Constant.MYSQL_DIM_TABLE)
                , WatermarkStrategy.<String>noWatermarks()
                , "table_process_dim_source").setParallelism(1);


        //将获取的维度表数据生成hbase表
        SingleOutputStreamOperator<TableProcessDim> hbaseDimCreateStream = table_process_dim_source.flatMap(new HBaseCreateDimRichFunction()).setParallelism(1);
        MapStateDescriptor<String, TableProcessDim> tableProcessDimMapStateDescriptor = new MapStateDescriptor<>("tableProcessDimMapStateDescriptor",
                String.class, TableProcessDim.class);
        //维度配置表写入到广播
        BroadcastStream<TableProcessDim> dimBroadcastStream = hbaseDimCreateStream.broadcast(tableProcessDimMapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = kafkaDimData.connect(dimBroadcastStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterDimStream = connectedStream.process(
                        new ConnectBroadcastProcessFunctionImpl(tableProcessDimMapStateDescriptor))
                .setParallelism(1);
        //修改data数据，只要hbase需要列的信息
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = getFilterColumnStream(filterDimStream);
        //写入到hbase
        filterColumnStream.addSink(
                new HBaseDataInsertFunction()
        );

    }
    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> getFilterColumnStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterDimStream) {
        return filterDimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> jsonObjectTableProcessDimTuple2) throws Exception {
                JSONObject kafkaDataJson = jsonObjectTableProcessDimTuple2.f0;
                TableProcessDim tableProcessDim = jsonObjectTableProcessDimTuple2.f1;
                List<String> columns = Arrays.asList(tableProcessDim.getSinkColumns().split(","));
                JSONObject data = kafkaDataJson.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return jsonObjectTableProcessDimTuple2;
            }
        });
    }


    private static SingleOutputStreamOperator<JSONObject> getKafkaDimData(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String jsonStrData, Collector<JSONObject> out) {
                try {
                    JSONObject jsonData = JSONObject.parseObject(jsonStrData);
                    String database = jsonData.getString("database");
                    String type = jsonData.getString("type");
                    JSONObject data = jsonData.getJSONObject("data");
                    //过滤想要的数据
                    if (Constant.MYSQL_WAREHOUSE_DB.equals(database) && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type) && data != null && !data.isEmpty()) {
                        out.collect(jsonData);
                    }
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        });
    }
}
