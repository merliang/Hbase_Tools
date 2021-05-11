package cn.edu.util;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

/**
 * @ClassName PropertiesUtil
 * @Author MerLiang
 * @Date 2021_04_27_16:31
 */
public class PropertiesUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);
    private static final Properties PROPS;

    static {
        String fileName = "util.properties";
        PROPS = new Properties();
        try {
            LOGGER.info("开始读取配置文件");
            PROPS.load(new InputStreamReader(Objects.requireNonNull(PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName)), "UTF-8"));
            LOGGER.info("配置文件读取完成");
        } catch (IOException e) {
            LOGGER.error("配置文件读取异常", e);
        }
    }

    public static Properties getProperties(String fileName) {
        Properties props = new Properties();
        try {
            props.load(new InputStreamReader(Objects.requireNonNull(PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName)), "UTF-8"));
        } catch (IOException e) {
            LOGGER.error("配置文件读取异常", e);
        }
        return props;
    }

    public static Properties getProperties() {
        return getProperties("hbase.properties");
    }

    public static String getProperty(String key) {
        String value = PROPS.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            return null;
        }
        return value.trim();
    }

    /**
     * 获取key，提供一个默认值，当key不存在的时候，提供默认值
     *
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getProperty(String key, String defaultValue) {
        String value = PROPS.getProperty(key.trim());
        // 判断value是否为空，如果为空，将value设置为默认值
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value.trim();
    }
}
