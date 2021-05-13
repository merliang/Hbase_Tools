package cn.edu.homework;

import cn.edu.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Test
 * @Author MerLiang
 * @Date 2021_04_28_10:51
 */
public class HbaseTest {
    private static Connection conn;
    private static Configuration conf;
    private static Admin admin;
    private static Table table;
    /**
     * 存储属性名和对应的值
     */
    static List<String> nameList;
    static List<Object> valueList;
    /**
     * 表名
     */
    static String TABLE_NAME = "Students";
    /**
     * 列名
     */
    static String COLUMN_NAME = "student";

    /**
     * 反射
     * @param o
     */
    static void hbaseTest(Object o) {
        Class<? extends Object> clazz = o.getClass();
        Field[] fields = clazz.getDeclaredFields();
        nameList = new ArrayList<>();
        valueList = new ArrayList<>();
        for (Field field : fields) {
            field.setAccessible(true);
            try {
                //获取属性名，用于建表时使用
                nameList.add(field.getName());
                //获取值
                valueList.add(field.get(o));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        //传入值
        Student student = new Student(1, "ml", "ml789456");
        hbaseTest(student);
        Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
        logger.info("创建表格");
        System.out.println(createTable(TABLE_NAME, COLUMN_NAME));
        logger.info("表创建结果:" + isTableExists(TABLE_NAME));
        logger.info("传入数据");
        logger.info(nameList + " " + valueList);
        System.out.println(putData(TABLE_NAME, valueList.get(0).toString(), COLUMN_NAME, valueList));
        logger.info("修改表中的数据");
        System.out.println(changeTable(TABLE_NAME, valueList.get(0).toString(), COLUMN_NAME, nameList.get(2), valueList.get(2).toString()));
        logger.info("删除数据");
        System.out.println(deleteData(TABLE_NAME, valueList.get(0).toString()));
        logger.info("删除表");
        System.out.println(dropTable(TABLE_NAME));
    }

    /**
     * 获取HBase连接对象
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        conn = ConnectionFactory.createConnection(conf);
        return conn;
    }

    /**
     * 获取Admin对象
     *
     * @return
     * @throws IOException
     */
    public static Admin getAdmin() throws IOException {
        conn = getConnection();
        admin = conn.getAdmin();
        return admin;
    }

    /**
     * 获取Table对象
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        conn = getConnection();
        TableName tableNames = TableName.valueOf(tableName);
        table = conn.getTable(tableNames);
        return table;
    }

    /**
     * 判断表是否存在
     *
     * @param tableNames
     * @return
     * @throws IOException
     */
    public static boolean isTableExists(String tableNames) throws IOException {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(tableNames);
        boolean result = admin.tableExists(tableName);
        return result;
    }

    /**
     * 建表
     *
     * @param tablesName
     * @param columnName
     * @return
     * @throws IOException
     */
    public static boolean createTable(String tablesName, String columnName) throws IOException {
        Admin admin = getAdmin();
        TableName name = TableName.valueOf(tablesName);
        ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName)).build();
        TableDescriptor dec = TableDescriptorBuilder.newBuilder(name).setColumnFamily(family).build();
        admin.createTable(dec);
        if (admin.tableExists(TableName.valueOf(tablesName))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 添加数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param student
     * @return
     * @throws IOException
     */
    public static boolean putData(String tableName, String rowKey, String familyName, List<Object> student) throws IOException {
        table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < nameList.size(); i++) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(nameList.get(i)), Bytes.toBytes(student.get(i).toString()));
        }
        table.put(put);
        table.close();
        return true;
    }

    /**
     * 修改表中的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param filed
     * @param data
     * @return
     * @throws IOException
     */
    public static boolean changeTable(String tableName, String rowKey, String family, String filed, String data) throws IOException {
        table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(filed), Bytes.toBytes(data));
        table.put(put);
        table.close();
        return true;
    }

    /**
     * 删除表中某一行数据
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws Exception
     */
    public static boolean deleteData(String tableName, String rowKey) throws Exception {
        table = getTable(tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (Exception e) {
            return false;
        } finally {
            table.close();
        }
        return true;

    }

    /**
     * 删除表
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean dropTable(String tableName) throws Exception {
        if (isTableExists(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
        return true;
    }
}
