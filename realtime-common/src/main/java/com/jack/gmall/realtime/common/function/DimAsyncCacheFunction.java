package com.jack.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.function.impl.DimAsyncCachejoin;
import com.jack.gmall.realtime.common.util.HBaseUtil;
import com.jack.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.function
 * @Author: lianchaoqi
 * @CreateTime: 2024-05-15  22:39
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public abstract class DimAsyncCacheFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncCachejoin<T> {
    private transient AsyncConnection hBaseAsyncConnection;
    private transient StatefulRedisConnection<String, String> redisAsyncConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();

    }

    @Override
    public void close() throws Exception {

        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);


    }

    @Override
    public void asyncInvoke(T T, ResultFuture<T> resultFuture) throws Exception {
        String rowKey = getId(T);
        String dimTab = getDimTabName();
        CompletableFuture.supplyAsync(new Supplier<JSONObject>() {
            @Override
            public JSONObject get() {
                System.out.println("woquredisl");
                //第一步 首先获取redis缓存的数据
                JSONObject jsonObject = null;
                try {
                    jsonObject = RedisUtil.readDimAsync(redisAsyncConnection, dimTab, rowKey);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return jsonObject;
            }
        }).thenApplyAsync(new Function<JSONObject, JSONObject>() {
            @Override
            public JSONObject apply(JSONObject jsonObject) {
                JSONObject readDimAsyncSku = null;
                //第二步 redis没有缓存的话 就去获取hbase数据
                if (jsonObject == null || jsonObject.size() == 0) {
                    System.out.println("woquhbasel");

                    readDimAsyncSku = HBaseUtil.readDimAsync(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, dimTab, rowKey);
                    //存一份到redis里面
                    if (!readDimAsyncSku.isEmpty()) {
                        RedisUtil.writeDimAsync(redisAsyncConnection, dimTab, rowKey, readDimAsyncSku);
                    }
                } else {
                    readDimAsyncSku = jsonObject;
                }
                return readDimAsyncSku;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                System.out.println("fuzhil");
                //关联维度信息
                if (jsonObject.size() != 0) {
                    dimJoin(T, jsonObject);
                } else {
                    System.out.println("没有该条数据维度信息");
                }
                resultFuture.complete(Collections.singletonList(T));
            }
        });
    }
}
