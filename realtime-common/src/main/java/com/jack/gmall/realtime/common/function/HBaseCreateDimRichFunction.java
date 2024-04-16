package com.jack.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.bean.TableProcessDim;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.io.Serializable;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.function
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-15  22:38
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class HBaseCreateDimRichFunction extends RichFlatMapFunction<String, TableProcessDim> implements Serializable {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
    }

    @Override
    public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            TableProcessDim tableProcessDim;
            //删除表
            String op = jsonObject.getString("op");
            if ("d".equals(op)) {
                tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                deleteTable(connection, tableProcessDim);
            } else if ("c".equals(op) || "r".equals(op)) {
                tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                createTable(connection, tableProcessDim);
            } else {
                tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                deleteTable(connection, tableProcessDim);
                createTable(connection, tableProcessDim);
            }
            tableProcessDim.setOp(op);
            out.collect(tableProcessDim);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.close(connection);
    }


    private void deleteTable(Connection connection, TableProcessDim tableProcessDim) {
        try {
            HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createTable(Connection connection, TableProcessDim tableProcessDim) {
        try {
            HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(),
                    tableProcessDim.getSinkColumns().split(","));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
