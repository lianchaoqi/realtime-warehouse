package com.jack.gmall.realtime.common.constant;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.constant
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-14  20:00
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class Constant {
    public static final String KAFKA_BROKERS = "hadoop101:9092,hadoop102:9092,hadoop103:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
//    public static final String DIM = "topic_log";

    public static final String MYSQL_HOST = "hadoop101";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "root";
    public static final String MYSQL_WAREHOUSE_DB = "gmall2023";
    public static final String MYSQL_DIM_CONGFIG_DB = "gmall2023_config";
    public static final String MYSQL_DIM_TABLE = "table_process_dim";
    public static final String HBASE_NAMESPACE = "gmall2023";
    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop101:2181,hadoop102:2181,hadoop103:2181";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop101:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES = "hadoop101:7030";

    public static final String DORIS_DATABASE = "gmall2023_realtime";
    public static final String DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DWS_TRADE_SKU_ORDER_WINDOW = "dws_trade_sku_order_window";
    public static final String DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    /*
    一天多少秒
     */
    public static final Integer DAY_SECONDS = 24 * 60 * 60;
    public static final Integer TWO_DAY_SECONDS = 2 * 24 * 60 * 60;

}
