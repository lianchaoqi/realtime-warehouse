package com.jack.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.jack.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                dim.put(key, value);
            }
            return dim;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

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
            System.out.println("插入数据：");
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

    public static JSONObject getCell(Connection connection
            , String namespace
            , String tableName
            , String rowKey) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        try {
            Result result = table.get(get);
            //返回的json对象
            for (Cell cell : result.rawCells()) {
                byte[] column = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                jsonObject.put(new String(column, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //关闭表对象
        table.close();

        return jsonObject;
    }

}
