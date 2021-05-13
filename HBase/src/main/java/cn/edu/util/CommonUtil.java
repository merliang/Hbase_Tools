package cn.edu.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @ClassName CommonUtil
 * @Author MerLiang
 * @Date 2021_05_10_10:42
 */
public class CommonUtil {
    private static Logger log = LoggerFactory.getLogger(CommonUtil.class);

    public static <T> Map<String, Object> convert(T bean){
        return convert(bean,true);
    }
    public static <T> Map<String, Object> convert(T bean,boolean isIgnoreNull){
        Map<String,Object> result = new HashMap<>();
        if(bean !=null){
            Class clazz = bean.getClass();
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                try {
                    Object o = field.get(bean);
                    if(!isIgnoreNull || o != null){
                        result.put(field.getName(),o.toString());
                    }
                } catch (IllegalAccessException e) {
                    log.error("访问权限异常",e);
                } catch (Exception e){
                    log.error("转换异常" ,e);
                }
            }
        }
        return result;
    }

    public static <E, T> List<E> convert(List<T> list, Function<T, E> func) {
        return list.stream().collect(ArrayList::new, (li, p) -> li.add(func.apply(p)), List::addAll);
    }

    public static <T> String getFieldValue(T bean,String fieldName) throws NoSuchFieldException, IllegalAccessException {
        if (bean != null) {
            Class clazz = bean.getClass();
            Field id = clazz.getDeclaredField(fieldName);
            id.setAccessible(true);
            Object value = id.get(bean);
            return value == null ? null : value.toString();
        }
        return null;
    }
}
