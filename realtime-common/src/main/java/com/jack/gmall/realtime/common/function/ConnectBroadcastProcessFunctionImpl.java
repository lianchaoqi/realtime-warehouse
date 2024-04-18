package com.jack.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.bean.TableProcessDim;
import com.jack.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.function
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-16  22:04
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class ConnectBroadcastProcessFunctionImpl extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private final Map<String, TableProcessDim> initDimInfoMap = new HashMap<>();
    private final MapStateDescriptor<String, TableProcessDim> tableProcessDimMapStateDescriptor;

    public ConnectBroadcastProcessFunctionImpl(MapStateDescriptor<String, TableProcessDim> tableProcessDimMapStateDescriptor) {
        this.tableProcessDimMapStateDescriptor = tableProcessDimMapStateDescriptor;
    }

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
            //TODO: 记得初始实例化对象之后再使用！！！！
            //TODO: 记得初始实例化对象之后再使用！！！！
            //TODO: 记得初始实例化对象之后再使用！！！！
            //TODO: 记得初始实例化对象之后再使用！！！！
            initDimInfoMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(tableProcessDimMapStateDescriptor);
        //这里将维度配置表的数据添加到广播状态
        String op = tableProcessDim.getOp();
        if ("d".equals(op)) {
            //如果是删除  则需要删除该状态
            broadcastState.remove(tableProcessDim.getSourceTable());
            //同步删除初始化map里面的维度表配置信息
            initDimInfoMap.remove(tableProcessDim.getSourceTable());
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
        //如果在状态里面拿不到，则去初始化配置里面看有没有  保证业务数据先来匹配不到配置表的情况
        if (tableProcessDim == null) {
            tableProcessDim = initDimInfoMap.get(sourceTable);
        }
        if (tableProcessDim != null) {
            //如果不为空，说明当前的业务数据是维度数据，下游输出
            collector.collect(Tuple2.of(kafkaTableData, tableProcessDim));
        }
    }
}
