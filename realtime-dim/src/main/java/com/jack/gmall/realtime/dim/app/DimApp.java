package com.jack.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.base.BaseApp;
import com.jack.gmall.realtime.common.bean.TableProcessDim;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.function.HBaseCreateDimRichFunction;
import com.jack.gmall.realtime.common.util.FlinkSourceUtil;
import com.jack.gmall.realtime.common.util.HBaseUtil;
import com.jack.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
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
        new DimApp().start(9999, 4, "DimApp", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //获取kafka的维度表业务数据
        SingleOutputStreamOperator<JSONObject> kafkaDimData = getKafkaDimData(stream);
//        //获取维度表的数据
        DataStreamSource<String> table_process_dim_source = env.fromSource(FlinkSourceUtil.getMySQLSource(Constant.MYSQL_DIM_CONGFIG_DB, Constant.MYSQL_DIM_TABLE), WatermarkStrategy.<String>noWatermarks(), "table_process_dim_source").setParallelism(1);


        //将获取的维度表数据生成hbase表
        SingleOutputStreamOperator<TableProcessDim> hbaseDimCreateStream = table_process_dim_source.flatMap(new HBaseCreateDimRichFunction());
//        hbaseDimCreateStream.print();
        MapStateDescriptor<String, TableProcessDim> tableProcessDimMapStateDescriptor = new MapStateDescriptor<>("tableProcessDimMapStateDescriptor",
                String.class, TableProcessDim.class);
        //维度配置表写入到广播
        BroadcastStream<TableProcessDim> dimBroadcastStream = hbaseDimCreateStream.broadcast(tableProcessDimMapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = kafkaDimData.connect(dimBroadcastStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterDimStream = connectedStream.process(

                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                    private HashMap<String, TableProcessDim> initDimInfoMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //程序启动前，在这里提前预加载配置表流，防止业务数据流来的比配置表流来的快，导致维度表数据丢失
                        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection
                                , "select source_table, sink_table, sink_family, sink_columns, sink_row_key from gmall2023_config.table_process_dim;"
                                , TableProcessDim.class
                                , true
                        );
                        for (TableProcessDim tableProcessDim : tableProcessDims) {
                            tableProcessDim.setOp("r");
                            initDimInfoMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }
                        JdbcUtil.closeConnection(mysqlConnection);
                    }
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(tableProcessDimMapStateDescriptor);
                        //这里将维度配置表的数据添加到广播状态
                        String op = tableProcessDim.getOp();
                        System.out.println(op);
                        if ("d".equals(op)) {
                            //如果是删除  则需要删除该状态
                            broadcastState.remove(tableProcessDim.getSourceTable());
                        } else {
                            broadcastState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }
                    }

                    @Override
                    public void processElement(JSONObject kafkaTableData, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        //这里需要过滤掉不是维度的数据
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(tableProcessDimMapStateDescriptor);
                        String sourceTable = kafkaTableData.getString("table");
                        TableProcessDim tableProcessDim = broadcastState.get(sourceTable);
                        if (tableProcessDim != null) {
                            //如果不为空，说明当前的业务数据是维度数据，下游输出
                            collector.collect(Tuple2.of(kafkaTableData, tableProcessDim));
                        }

                    }


                }
        );

        filterDimStream.print();

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
