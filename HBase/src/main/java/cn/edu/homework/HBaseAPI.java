package cn.edu.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * @ClassName HBaseAPI
 * @Author MerLiang
 * @Date 2021_04_27_16:29
 */
public class HBaseAPI {
    Connection conn;
    private Configuration conf;
    private Admin admin;
    private Table table;

    @Before
    public void init() throws IOException {
        Scanner scan = new Scanner(System.in);
        String tableName = scan.nextLine();
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        //获取HBase对象
        conn = ConnectionFactory.createConnection(conf);
        //创建一个Table对象
        table = conn.getTable(TableName.valueOf(tableName));
        //获取Admin对象
        admin = conn.getAdmin();
    }


    /**
     * 判断表是否存在
     */
    @Test
    public void isTableExists(String strs) throws IOException {
        TableName tableName = TableName.valueOf(strs);
        boolean result = admin.tableExists(tableName);
        System.out.println(result);
    }

    /**
     * 创建表
     */
    @Test
    public void createTable(String tablesName, String columnName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tablesName))) {
            System.out.println("表" + tablesName + "已存在");
            System.exit(0);
        } else {
            TableName name = TableName.valueOf(tablesName);
            List<ColumnFamilyDescriptor> families = new ArrayList<>();
            ColumnFamilyDescriptor family1 = ColumnFamilyDescriptorBuilder.of("info");
            ColumnFamilyDescriptor family2 = ColumnFamilyDescriptorBuilder.of(columnName);
            families.add(family1);
            families.add(family2);
            TableDescriptor dec = TableDescriptorBuilder.newBuilder(name).setColumnFamilies(families).build();
            admin.createTable(dec);
            if (admin.tableExists(TableName.valueOf(tablesName))) {
                System.out.println("创建成功");
            }
        }
    }

    /**
     * 修改表数据
     */
    @Test
    public void modifyTablesData(String rowKey, String columnName1, String data1, String columnName2, String data2) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(columnName1), Bytes.toBytes(data1));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(columnName2), Bytes.toBytes(data2));
        table.put(put);
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable(String tableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("删除成功");
        } else {
            System.out.println("无此表");
        }
    }
}
