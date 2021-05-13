package cn.edu.util;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @ClassName HBaseUtil
 * @Author MerLiang
 * @Date 2021_05_10_11:07
 */
public class HBaseUtil {
    private static Connection conn;
    private static Properties props;
    private static Logger log = LoggerFactory.getLogger(HBaseUtil.class);


    /**
     * 读取配置文件，连接Hbase
     */
    static {
        props = new Properties();
        try {
            props.load(new InputStreamReader(Objects.requireNonNull(HBaseUtil.class.getClassLoader().getResourceAsStream("hbase.properties")), "UTF-8"));
        } catch (IOException e) {
            log.error("配置文件读取异常", e);
        }
        Configuration conf = HBaseConfiguration.create();
        String value = props.getProperty("hbase.zookeeper.quorum");
        if (StringUtils.isNotBlank(value)) {
            conf.set("hbase.zookeeper.quorum", value.trim());
        }
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("获取HBase连接异常", e);
        }
    }

    /**
     * 获取Hbase的无参连接
     *
     * @return
     */
    public static Connection getConnection() {
        return conn;
    }

    /**
     * Hbase的有参连接
     *
     * @param quorum
     * @return
     */
    public static Connection getConnection(String quorum) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("获取HBase连接异常", e);
        }
        return null;
    }

    /**
     * 设置有参Hbase连接对象
     *
     * @param connection
     */
    public static void setConnection(Connection connection) {
        conn = connection;
    }

    /**
     * 先判断Hbase的连接是否正常，再获取Admin
     *
     * @return
     */
    public static Admin getAdmin() {
        if (check()) {
            try {
                return conn.getAdmin();
            } catch (IOException e) {
                log.error("获取Admin对象异常", e);
            }
        }
        return null;
    }

    /**
     * 先判断Hbase的连接是否正常，再获取表
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) {
        if (check()) {
            try {
                return conn.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                log.error("获取Table对象异常", e);
            }
        }
        return null;
    }

    /**
     * 创建表格 表名，列族，最小版本号，最大版本号
     *
     * @param tableName
     * @param family
     * @param minVersion
     * @param maxVersion
     * @return
     */
    public static boolean createTable(String tableName, String family, int minVersion, int maxVersion) {
        List<String> families = new LinkedList();
        families.add(family);
        return createTable(tableName, families, minVersion, maxVersion);
    }

    /**
     * 创建表格 表名，列族，版本号
     *
     * @param tableName
     * @param family
     * @param version
     * @return
     */
    public static boolean createTable(String tableName, String family, int version) {
        return createTable(tableName, family, version, version);
    }

    /**
     * 创建表格 表名，列族，默认版本号1
     *
     * @param tableName
     * @param family
     * @return
     */
    public static boolean createTable(String tableName, String family) {
        return createTable(tableName, family, 1, 1);
    }

    /**
     * 创建表格 表名，最小版本号，最大版本号，可变参数列族
     *
     * @param tableName
     * @param minVersion
     * @param maxVersion
     * @param family
     * @return
     */
    public static boolean createTable(String tableName, int minVersion, int maxVersion, String... family) {
        List<String> families = Arrays.asList(family);
        return createTable(tableName, families, minVersion, maxVersion);
    }

    /**
     * 创建表格 表名，版本号，可变参数列族
     *
     * @param tableName
     * @param version
     * @param family
     * @return
     */
    public static boolean createTable(String tableName, int version, String... family) {
        return createTable(tableName, version, version, family);
    }

    /**
     *创建表格 表名，可变参数列族，默认版本号1
     *
     * @param tableName
     * @param family
     * @return
     */
    public static boolean createTable(String tableName, String... family) {
        return createTable(tableName, 1, 1, family);
    }

    /**
     *创建表格 表名，String类型的List集合列族，最小版本号，最大版本号
     *
     * @param tableName
     * @param families
     * @param minVersion
     * @param maxVersion
     * @return
     */
    public static boolean createTable(String tableName, List<String> families, int minVersion, int maxVersion) {
        if (families == null || families.isEmpty()) {
            log.error("列族数据为空，创建表失败");
            return false;
        }
        Admin admin = getAdmin();
        boolean status = false;
        checkVersion(minVersion, maxVersion);
        if (admin != null) {
            List<ColumnFamilyDescriptor> familyDescriptors = CommonUtil.convert(families, family -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).setMinVersions(minVersion).setMaxVersions(maxVersion).build());
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamilies(familyDescriptors).build();

            try {
                admin.createTable(desc);
                status = true;
            } catch (IOException e) {
                log.error("创建表操作异常", e);
            } finally {
                if (admin != null) {
                    try {
                        admin.close();
                    } catch (IOException e) {
                        log.error("关闭Admin对象异常", e);
                    }
                }
            }
        }
        return status;
    }

    /**
     * 创建表格 表名，String类型的List集合列族，版本号
     *
     * @param tableName
     * @param families
     * @param version
     * @return
     */
    public static boolean createTable(String tableName, List<String> families, int version) {
        return createTable(tableName, families, 1, version);
    }

    /**
     * 创建表格 表名，String类型的List集合列族
     *
     * @param tableName
     * @param families
     * @return
     */
    public static boolean createTable(String tableName, List<String> families) {
        return createTable(tableName, families, 1);
    }

    /**
     * 添加行 表名，行名，列名，Map<String, Object> map
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param map
     * @return
     */
    public static boolean putByRowKey(String tableName, String rowKey, String family, Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            log.warn("数据为空，未添加任何数据");
            return false;
        }
        Table table = getTable(tableName);
        boolean result = false;
        if (table != null) {
            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                //map的key是列名，value是列值
                map.forEach((key, value) -> {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value == null ? "null" : value.toString()));
                });
                table.put(put);
                result = true;
            } catch (IOException e) {
                log.error("插入数据异常", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭表对象异常", e);
                }
            }
        }
        return result;
    }

    public static boolean putByRowKey(String tableName, String rowKey, String family, String column, Object value) {
        Table table = getTable(tableName);
        boolean result = false;
        if (table != null) {
            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value == null ? "null" : value.toString()));
                table.put(put);
                result = true;
            } catch (IOException e) {
                log.error("插入数据异常", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭表对象异常", e);
                }
            }
        }
        return result;
    }

    public static <T> boolean putByRowKey(String tableName, String rowKey, String family, T bean) {
        return putByRowKey(tableName, rowKey, family, CommonUtil.convert(bean));
    }

    public static <T> boolean put(String tableName, String family, T bean) {
        String rowKey;
        try {
            rowKey = CommonUtil.getFieldValue(bean, "id");
            if (rowKey == null) {
                log.error("对象的id字段数据为空，添加数据失败");
                return false;
            }
        } catch (NoSuchFieldException e) {
            log.error("对象必须有id字段，但是接收到的对象不具备id字段", e);
            return false;
        } catch (IllegalAccessException e) {
            log.error("字段id不具备访问权限", e);
            return false;
        }
        return putByRowKey(tableName, rowKey, family, CommonUtil.convert(bean));
    }

    public static <T> boolean puts(String tableName, String family, List<T> beans) {
        if (beans == null || beans.isEmpty()) {
            log.warn("数据为空，未添加任何数据");
            return false;
        }
        Table table = getTable(tableName);
        boolean result = false;
        if (table != null) {
            try {
                List<Put> puts = new ArrayList<>();
                for (T bean : beans) {
                    puts.add(convertToPut(bean, family));
                }
                table.put(puts);
                result = true;
            } catch (IOException e) {
                log.error("插入数据异常", e);
            } catch (NoSuchFieldException e) {
                log.error("对象必须有id字段，但是接收到的对象不具备id字段", e);
            } catch (IllegalAccessException e) {
                log.error("字段 id 不具备访问权限", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭表对象异常", e);
                }
            }
        }
        return result;
    }

    public static <T> T getBeanByRowKey(String tableName, String rowKey, String family, Class<T> beanType) {
        Table table = getTable(tableName);
        T t = null;
        if (table != null) {
            try {
                Result result = table.get(new Get(Bytes.toBytes(rowKey)));
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
                Constructor<T> constructor = beanType.getConstructor();
                t = constructor.newInstance();
                for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    try {
                        Field field = beanType.getDeclaredField(Bytes.toString(entry.getKey()));
                        field.setAccessible(true);
                        field.set(t, ConvertUtils.convert(Bytes.toString(entry.getValue()), field.getType()));
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        log.warn("{} 不是 {} 的成员", Bytes.toString(entry.getKey()), beanType);
                    }
                }
            } catch (IOException e) {
                log.error("根据 RowKey 获取数据失败", e);
            } catch (IllegalAccessException e) {
                log.error("实例化Bean对象异常，BeanType没有公共的无参构造器", e);
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                log.error("BeanType必须具备无参构造器, 而当前对象不具备无参构造器", e);
            } catch (InstantiationException e) {
                log.error("实例化Bean对象异常", e);
            }
        }
        return t;
    }

    /**
     * 检查Hbase是否连接成功
     *
     * @return
     */
    private static boolean check() {
        if (conn == null) {
            log.error("创建HBase连接失败，请重新配置HBase的Zookeeper地址");
            return false;
        }
        return true;
    }

    /**
     * 检查版本号是否错误
     *
     * @param minVersion
     * @param maxVersion
     * @throws IllegalArgumentException
     */
    private static void checkVersion(int minVersion, int maxVersion) throws IllegalArgumentException {
        if (minVersion > maxVersion) {
            throw new IllegalArgumentException(maxVersion + "必须大于等于" + minVersion);
        }
        if (minVersion < 1) {
            throw new IllegalArgumentException(minVersion + "必须大于等于1");
        }
    }

    /**
     * 将byte数组类型的Map转化为String类型的Map
     *
     * @param map
     * @return
     */
    private static Map<String, String> convert(Map<byte[], byte[]> map) {
        Map<String, String> result = new HashMap<>();
        if (map != null) {
            map.forEach((byte[] k, byte[] v) -> result.put(Bytes.toString(k), Bytes.toString(v)));
        }
        return result;
    }

    /**
     * @param bean
     * @param family
     * @param <T>
     * @return
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private static <T> Put convertToPut(T bean, String family) throws NoSuchFieldException, IllegalAccessException {
        Put put = new Put(Bytes.toBytes(CommonUtil.getFieldValue(bean, "id")));
        CommonUtil.convert(bean).forEach((key, value) -> put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value == null ? "null" : value.toString())));
        return put;
    }
}
