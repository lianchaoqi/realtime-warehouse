package com.jack.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.bean.TableProcessDim;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.HBaseUtil;
import com.jack.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.function
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-17  00:19
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class HBaseDataInsertFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection connection;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.close(connection);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject kafkaJsonObject = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        String type = kafkaJsonObject.getString("type");
        JSONObject data = kafkaJsonObject.getJSONObject("data");
        //type只有 insert，update，delete，bootstrap-insert
        if ("delete".equals(type)) {
            deleteData(data, tableProcessDim);
        } else {
            putData(data, tableProcessDim);
        }

        //判断redis中的key有没发生变化

        if ("delete".equals(type) || "update".equals(type)) {
            //删除掉该变动的key的值 保证数据一致性
            jedis.del(RedisUtil.getKey(
                    tableProcessDim.getSinkTable(), data.getString(tableProcessDim.getSinkRowKey())
            ));
        }
    }

    private void putData(JSONObject data, TableProcessDim tableProcessDim) {
        String rowKey = data.getString(tableProcessDim.getSinkRowKey());
        try {
            HBaseUtil.putCells(connection
                    , Constant.HBASE_NAMESPACE
                    , tableProcessDim.getSinkTable()
                    , rowKey
                    , tableProcessDim.getSinkFamily()
                    , data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteData(JSONObject data, TableProcessDim tableProcessDim) {
        try {
            String rowkey = data.getString(tableProcessDim.getSinkRowKey());
            HBaseUtil.deleteCells(connection
                    , Constant.HBASE_NAMESPACE
                    , tableProcessDim.getSinkTable()
                    , rowkey);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
