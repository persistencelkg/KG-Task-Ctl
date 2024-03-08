package org.kg.ctl.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Set;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/5 2:30 PM
 */
@Service
@Slf4j
public class CheckService {

    public boolean check(Object from, Object to, Set<String> excludeFields, String uniqueKey) {
        Object uniqueValue = null;
        try {
            Class<?> fromClass = from.getClass();
            Class<?> targetClass = to.getClass();
            uniqueValue = getFieldValue(from, fromClass, uniqueKey);
            // 优先比较update Time，
            if (!compareEachOther(from, to, fromClass, targetClass, "updateTime", uniqueKey)) {
                log.warn("{}: update_time not satisfy check condition", uniqueValue);
                return true;
            }
            Field[] declaredFields = from.getClass().getDeclaredFields();
            boolean baseFilter = !CollectionUtils.isEmpty(excludeFields);
            for (Field declaredField : declaredFields) {
                String name = declaredField.getName();
                if (baseFilter && excludeFields.contains(name)) {
                    continue;
                }
                if (!compareEachOther(from, to, fromClass, targetClass, name, uniqueKey)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.error("{}, check error", uniqueValue, e);
            return false;
        }

    }

    private boolean compareEachOther(Object from, Object to, Class<?> fromClass, Class<?> targetClass, String fieldName, String uniqueValue) {
        try {
            Object fromVal = getFieldValue(from, fromClass, fieldName);
            Object toVal = getFieldValue(to, targetClass, fieldName);
            if (!Objects.equals(fromVal, toVal)) {
                log.warn("key:{} field: {}, check fail, from val:{}, to val:{}", uniqueValue, fieldName, fromVal, toVal);
                return false;
            }
        } catch (Exception e) {
            log.warn("key:{} field:{} check error:", uniqueValue, fieldName, e);
            return false;
        }
        return true;
    }

    public static Object getFieldValue(Object from, Class<?> fromClass, String fieldName) {
        Field declaredField = null;
        try {
            declaredField = fromClass.getDeclaredField(fieldName);
            declaredField.setAccessible(true);
            return declaredField.get(from);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
