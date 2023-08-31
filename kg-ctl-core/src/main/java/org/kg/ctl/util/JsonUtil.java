package org.kg.ctl.util;

import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 项目日期类统一使用 jsr310 格式, 指定序列化反序列化的格式
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        // 设置忽略在JSON字符串中存在但Java对象实际没有的属性
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 设置其他全局配置，例如格式化输出
//        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        objectMapper.registerModules(new ParameterNamesModule(), new Jdk8Module(), javaTimeModule);
    }

    public static String toJson(Object bean) {
        if (Objects.isNull(bean)) {
            return null;
        }
        if (bean instanceof String) {
            return (String) bean;
        }
        try {
            return objectMapper.writeValueAsString(bean);
        } catch (JsonProcessingException e) {
            log.error("JsonUtil.toJson 错误", e);
            return null;
        }
    }

    public static byte[] toBinary(Object bean) {
        String json = toJson(bean);
        if (json == null || StringUtils.isBlank(json)) {
            return null;
        }
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T toBean(byte[] json, Class<T> valueType) {
        return toBean(new String(json, StandardCharsets.UTF_8), valueType);
    }

    public static <T> T toBean(byte[] json, TypeReference<T> typeReference) {
        return toBean(new String(json), typeReference);
    }

    public static <T> T toBean(String json, Class<T> valueType) {
        try {
            return objectMapper.readValue(json, valueType);
        } catch (IOException e) {
            log.error("JsonUtil.toBean 错误, json: {}, Bean: {}", json, valueType.getSimpleName(), e);
            return null;
        }
    }

    public static <T> T toBean(String json, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (IOException e) {
            log.error("JsonUtil.toBean 错误, json: {}", json, e);
            return null;
        }
    }
}
