package cn.edu.zut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName HBaseAdminOperate
 * @Author MerLiang
 * @Date 2021_04_26_15:23
 */

public class HBaseAdminOperate {
    private Configuration conf;
    private Admin admin;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    /*判断表是否存在*/
    @Test
    public void isTableExists() throws IOException {
        TableName table = TableName.valueOf("student");
        boolean result = admin.tableExists(table);
        System.out.println(result);
    }

    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        String tableName = "Students";
        String[] columnFamily = {"info", "education"};
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表已存在");
            System.exit(0);
        } else {
            TableName name = TableName.valueOf(tableName);
            List<ColumnFamilyDescriptor> families = new ArrayList<>();
            ColumnFamilyDescriptor family1 = ColumnFamilyDescriptorBuilder.of(columnFamily[0]);
            ColumnFamilyDescriptor family2 = ColumnFamilyDescriptorBuilder.of(columnFamily[1]);
            families.add(family1);
            families.add(family2);
            TableDescriptor dec = TableDescriptorBuilder.newBuilder(name).setColumnFamilies(families).build();
            admin.createTable(dec);
            // System.out.println(admin.tableExists(TableName.valueOf("teacher")));
            TableName[] tableNames = admin.listTableNames();
            Arrays.stream(tableNames).forEach(System.out::println);
        }
    }

    /**
     * 删除表
     *
     * @throws IOException
     */
    @Test
    public void dropTable() throws IOException {
        String tableName = "Students";
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("删除成功");
        } else {
            System.out.println("表不存在");
        }
    }

    /*查询数据库中所有的表*/
    @Test
    public void listTables() throws IOException {
        TableName[] ts = admin.listTableNames();
        for (TableName t : ts) {
            System.out.println(t);
        }
    }
}
