package cn.edu.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName HBaseUtil
 * @Author MerLiang
 * @Date 2021_05_10_11:07
 */
public class HBaseUtil {
    private static Connection CONN;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        try {
            CONN = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 保存数据，表名，行，列族，对象
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param bean
     * @param <T>
     * @return
     */
    public static <T> boolean saveBean(String tableName, String rowKey, String family, T bean) {
        boolean status = false;
        try {
            Table table = CONN.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            Map<String, Object> datas = CommonUtil.convert(bean);
            datas.forEach((key, value) -> {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toString(value));
                try {
                    table.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static <T> T get(String tableName, String rowKey, String family,Class<T> beanType) {
        boolean status = false;
        Table table = null;
        try {
            table = CONN.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            Map<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(family));
            Map<String, Object> data = new HashMap<>();
            map.forEach((key, value) -> {
                data.put(Bytes.toString(key), Bytes.toString(value));
            });
            BeanUtils.populate();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
