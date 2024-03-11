package org.kg.ctl.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TaskUtil {

    private static final Pattern pattern = Pattern.compile("(?<=[a-z])([A-Z])");

    public static TemporalAmount buildDurationPeriod(String key) {
        return key.contains("T") ? buildTaskDuration(key) : buildTaskPeriod(key);
    }

    public static Duration buildTaskDuration(String period) {
        return buildTaskDuration(period, 1);
    }

    public static Duration buildTaskDuration(String period, int defaultValue) {
        if (ObjectUtils.isEmpty(period)) {
            return Duration.ofMinutes(defaultValue);
        }
        return Duration.parse(period);
    }


    public static Period buildTaskPeriod(String period) {
        return buildTaskPeriod(period, 1);
    }

    public static Period buildTaskPeriod(String period, int defaultValue) {
        if (ObjectUtils.isEmpty(period)) {
            return Period.ofDays(defaultValue);
        }
        return Period.parse(period);
    }

    public static List<Integer> list(int end) {
        return list(0, end);
    }

    /**
     * 左闭右开
     *
     * @param start
     * @param end
     * @return
     */
    public static List<Integer> list(int start, int end) {
        if (start >= end) {
            return null;
        }
        ArrayList<Integer> arrayList = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            arrayList.add(i);
        }
        return arrayList;
    }


    /**
     * 每个机器属于自己执行的部分
     *
     * @param list
     * @param index
     * @param totalCount
     * @param <T>
     * @return
     */
    public static <T> List<T> list(List<T> list, Integer index, Integer totalCount) {
        // 任务分发
        int total = list.size();
        // 单机情况
        if (list.size() < totalCount || totalCount < 2) {
            return list;
        }
        int shardIndex = index;
        // 批次
        int batchSize = total / totalCount;
        int leaveSize = (total & (totalCount - 1));
        int from = shardIndex * batchSize;
        if (leaveSize != 0 && index == totalCount - 1) {
            return list.subList(from, total);
        }
        return list.subList(from, from + batchSize);
    }

    public static String getPrefixWithOutUnderLine(String tableName) {
        int i = tableName.lastIndexOf("_");
        if (i < 0 || !tableName.endsWith("_")) {
            return tableName;
        }
        return tableName.substring(0, i);
    }

    public static String underLineToCamel(String str) {
        StringBuilder sb = new StringBuilder();
        String[] split = str.split("_");
        for (int i = 0; i < split.length; i++) {
            String word = split[i];
            if (i == 0) {
                sb.append(word);
            } else {
                sb.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
            }
        }
        return sb.toString();
    }

    public static String camelToUnderLine(String input) {
        Matcher matcher = pattern.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, "_" + matcher.group(1).toLowerCase());
        }
        matcher.appendTail(sb);
        return sb.toString();
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TimeSegment {
        LocalDateTime start;
        LocalDateTime end;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class IdSegment {
        Integer start;
        Integer end;
    }

    public static void main(String[] args) {
        System.out.println(underLineToCamel("helloWorldJava"));
        System.out.println(camelToUnderLine("hello_world_java"));
    }
}
