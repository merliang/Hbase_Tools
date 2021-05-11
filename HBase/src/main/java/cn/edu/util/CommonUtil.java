package cn.edu.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName CommonUtil
 * @Author MerLiang
 * @Date 2021_05_10_10:42
 */
public class CommonUtil {
    // 使用log

    public static void main(String[] args) {
        Student student = new Student("v", 1, "cwsad");

    }

    public static <E> Map<String, Object> convert(E bean) {
        Map<String, Object> result = new HashMap<>();
        // 获取字节码文件对象
        Class clazz = bean.getClass();
        // 反射,获取所有字段
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String key = field.getName();
            try {
                Object value = field.get(bean);
                if (value != null) {
                    result.put(key, value);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
