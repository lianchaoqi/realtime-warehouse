package com.jcak.gmall.realtime.dws.function;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.HBaseUtil;
import com.jack.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jcak.gmall.realtime.dws.function
 * @Author: lianchaoqi
 * @CreateTime: 2024-05-15  10:13
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class RedisAsyncGetRichMapfunction extends RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean> {

    private transient Connection connection;
    private transient Jedis jedis;

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
    public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
        //凭借redis的完整key  规范
        String dimSkuInfoKey = RedisUtil.getKey("dim_sku_info", tradeSkuOrderBean.getSkuId());
        //获取redis key对应的值
        String skuInfoJson = jedis.get(dimSkuInfoKey);
        JSONObject jsonObject;
        if (skuInfoJson == null || skuInfoJson.length() == 0) {
            //redis无缓存 获取hbase数据
            jsonObject = HBaseUtil.getCell(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", tradeSkuOrderBean.getSkuId());
            //把获取到的数据存到hbase一份
            if (jsonObject.size() != 0) {
                //key, 保留时间, value
                jedis.setex(dimSkuInfoKey, Constant.DAY_SECONDS, jsonObject.toJSONString());
            }
        } else {
            //redis有缓存  直接取
            jsonObject = JSONObject.parseObject(skuInfoJson);
        }

        if (jsonObject.size() != 0) {
            tradeSkuOrderBean.setCategory3Id(jsonObject.getString("category3_id"));
            tradeSkuOrderBean.setTrademarkId(jsonObject.getString("tm_id"));
            tradeSkuOrderBean.setSpuId(jsonObject.getString("spu_id"));
            tradeSkuOrderBean.setSkuName(jsonObject.getString("sku_name"));
        } else {
            System.out.println("没有该条数据维度信息");
        }
        return tradeSkuOrderBean;
    }

}
