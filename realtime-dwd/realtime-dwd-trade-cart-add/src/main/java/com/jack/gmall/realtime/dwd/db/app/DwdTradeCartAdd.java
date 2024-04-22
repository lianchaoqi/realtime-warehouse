package com.jack.gmall.realtime.dwd.db.app;

import com.jack.gmall.realtime.common.base.BaseSQLApp;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.dwd.db.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-20  23:53
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                11004,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        createKafkaTopicDBTab(ckAndGroupId, tableEnv);
        // 2. 过滤出加购数据
        Table cartAdd = tableEnv.sqlQuery(
                "select " +
                        " `data`['id'] id," +
                        " `data`['user_id'] user_id," +
                        " `data`['sku_id'] sku_id," +
                        " `data`['cart_price'] cart_price," +
                        " `data`['sku_name'] sku_name," +
                        " `data`['create_time'] create_time," +
                        " `data`['operate_time'] operate_time," +
                        " `data`['is_ordered'] is_ordered," +
                        " `data`['order_time'] order_time," +
                        " `data`['source_type'] source_type," +
                        " `data`['source_id'] source_id," +
                        " if(`type`='insert'," +
                        "   cast(`data`['sku_num'] as int), " +
                        "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                        ") sku_num ," +
                        " ts " +
                        "from topic_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='cart_info' " +
                        "and (" +
                        " `type`='insert' " +
                        "  or(" +
                        "     `type`='update' " +
                        "      and `old`['sku_num'] is not null " +
                        "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                        "   )" +
                        ")");

        // 3. 写出到 kafka
        tableEnv.executeSql("create table dwd_trade_cart_add(" +
                "   id string, " +
                "   user_id string," +
                "   sku_id string," +
                "   cart_price string, " +
                "   sku_name string, " +
                "   create_time string, " +
                "   operate_time string, " +
                "   is_ordered string, " +
                "   order_time string, " +
                "   source_type string, " +
                "   source_id string, " +
                "   sku_num bigint, " +
                "   ts  bigint " +
                ")" + FlinkSQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}

