package com.jack.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.util
 * @Author: lianchaoqi
 * @CreateTime: 2024-04-15  13:25
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class HBaseUtil {
    public static Connection getConnection() {
        Connection connection = null;
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    public static void close(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable(Connection connection, String namespace, String tableName, String... columnFamilies) throws IOException {
        if (columnFamilies.length == 0 || columnFamilies == null) {
            System.out.println("列族不能为空");
            return;
        }
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(namespace, tableName))) {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(columnFamily)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            try {
                admin.createTable(tableDescriptorBuilder.build());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println(namespace + "." + tableName + "表已存在");
        }
        admin.close();
    }

    /*
    删除表
     */
    public static void dropTable(Connection connection, String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(namespace, tableName));
            admin.deleteTable(TableName.valueOf(namespace, tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        admin.close();
    }

    /**
     * 写入数据
     *
     * @param connection:     链接
     * @param namespace：命名空间
     * @param tableName：表名
     * @param rowKey：主键
     * @param columnFamily：列族
     * @param data：列值
     */
    public static void putCells(Connection connection
            , String namespace
            , String tableName
            , String rowKey
            , String columnFamily
            , JSONObject data
    ) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //插入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : data.keySet()) {
            String value = data.getString(column);
            //避免value值为空，空的不写入
            if (value != null) {
                put.addColumn(Bytes.toBytes(columnFamily)
                        , Bytes.toBytes(column)
                        , Bytes.toBytes(value));
            }
        }
        //写出数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }


    /**
     * 删除数据
     *
     * @param connection:    链接
     * @param namespace：命名空间
     * @param tableName：表名
     * @param rowKey：主键
     */
    public static void deleteCells(Connection connection
            , String namespace
            , String tableName
            , String rowKey
    ) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        table.close();
    }


}
