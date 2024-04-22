package com.jack.gmall.realtime.dwd.db.app;

import com.jack.gmall.realtime.common.base.BaseSQLApp;
import com.jack.gmall.realtime.common.constant.Constant;
import com.jack.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.dwd.db.app
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-19  17:14
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        DwdInteractionCommentInfo dwdInteractionCommentInfo = new DwdInteractionCommentInfo();
        new DwdInteractionCommentInfo().start(11003
                , 4
                , "DwdInteractionCommentInfo");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        //核心处理逻辑
        //1.创建HBase维度表
        createHBaseDimBaseDicData(tableEnv);
        //2.创建KafkaTopicDB表
        createKafkaTopicDBTab(ckAndGroupId, tableEnv);
        Table commentInfo = tableEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['sku_id'] sku_id, " +
                        " `data`['appraise'] appraise, " +
                        " `data`['comment_txt'] comment_txt, " +
                        " `data`['create_time'] comment_time," +
                        "  ts, " +
                        " proc_time " +
                        " from topic_db " +
                        " where `database`='gmall2023' " +
                        " and `table`='comment_info' " +
                        " and `type`='insert' ");
        tableEnv.createTemporaryView("comment_info", commentInfo);
        Table tableResult = tableEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id," +
                "ci.sku_id," +
                "ci.appraise," +
                "dic.info.dic_name appraise_name," +
                "ci.comment_txt," +
                "ci.ts " +
                "from comment_info ci " +
                "join dim_base_dic for system_time as of ci.proc_time as dic " +
                "on ci.appraise=dic.rowkey");

        // 5. 通过 ddl 方式建表: 与 kafka 的 topic 管理 (sink)
        tableEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id string, " +
                "user_id string," +
                "sku_id string," +
                "appraise string," +
                "appraise_name string," +
                "comment_txt string," +
                "ts bigint " +
                ")" + FlinkSQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 6. 把 join 的结果写入到 sink 表
        tableResult.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
