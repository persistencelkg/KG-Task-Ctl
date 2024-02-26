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

    public boolean check(Object from, Object to, Set<String> excludeFields) {
        try {
            Class<?> fromClass = from.getClass();
            Class<?> targetClass = to.getClass();
            // 优先比较update Time，
            if (!compareEachOther(from, to, fromClass, targetClass, "updateTime")) {
                log.warn("update_time not satisfy check condition");
                return false;
            }
            Field[] declaredFields = from.getClass().getDeclaredFields();
            boolean baseFilter = !CollectionUtils.isEmpty(excludeFields);
            for (Field declaredField : declaredFields) {
                String name = declaredField.getName();
                if (baseFilter && excludeFields.contains(name)) {
                    continue;
                }
                if (!compareEachOther(from, to, fromClass, targetClass, name)) {
                    log.error("{} check not consist！from :{} \n to{}", name, from, to);
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.error("check", e);
            return false;
        }

    }

    private boolean compareEachOther(Object from, Object to, Class<?> fromClass, Class<?> targetClass, String fieldName) {
        try {
            Object fromVal = getFieldValue(from, fromClass, fieldName);
            Object toVal = getFieldValue(to, targetClass, fieldName);
            if (!Objects.equals(fromVal, toVal)) {
                log.warn("field: {}, check fail, from val:{}, to val:{}", fieldName, fromVal, toVal);
                return false;
            }
        } catch (Exception e) {
            log.warn("field:{} check error:", fieldName, e);
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
