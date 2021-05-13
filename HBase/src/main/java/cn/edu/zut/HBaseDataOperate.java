package cn.edu.zut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ClassName HBaseDataOperate
 * @Author MerLiang
 * @Date 2021_04_26_15:50
 */
public class HBaseDataOperate {
    Connection conn;
    private Table table;

    /*初始化*/
    @Before
    public void init() throws IOException {
        String tableName = "student";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(tableName));
    }

    @After
    public void close() throws IOException {
        table.close();
        conn.close();
    }

    /*添加数据*/
    @Test
    public void insert() throws IOException {
        Put put = new Put(Bytes.toBytes("1003"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("王五"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("16"));
        table.put(put);
    }

    /*删除数据*/
    @Test
    public void delete() throws IOException {
        Delete del = new Delete(Bytes.toBytes("1003"));
        del.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        table.delete(del);
    }

    /*查询数据*/
    @Test
    public void query() throws IOException {
        Get get = new Get(Bytes.toBytes("1002"));
        //指定获取age列数据
        // get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));

        Result result = table.get(get);
        for (Cell c : result.rawCells()) {
            System.out.print("RowKey: " + Bytes.toString(CellUtil.cloneRow(c)) + " ");
            System.out.print("时间戳：" + c.getTimestamp() + " ");
            System.out.print("列名: " + new String(CellUtil.cloneQualifier(c)) + " ");
            System.out.print("值: " + new String(CellUtil.cloneValue(c)));
        }
    }

    /*扫描查询数据*/
    @Test
    public void scan() throws IOException {
        String beginRowKey = "1001";
        String endRowKey = "1004";
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(beginRowKey), true);
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setCaching(20);
        scan.setBatch(10);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (Cell c : r.rawCells()) {
                System.out.print("RowKey: " + Bytes.toString(CellUtil.cloneRow(c)) + " ");
                System.out.print("时间戳：" + c.getTimestamp() + " ");
                System.out.print("列名: " + new String(CellUtil.cloneQualifier(c)) + " ");
                System.out.print("值: " + new String(CellUtil.cloneValue(c)));
                System.out.println();
            }
        }
        rs.close();
    }
}
