package org.kg.ctl.util;

import javafx.concurrent.Task;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.service.BatchSaveSegmentTaskEvent;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TaskUtil {

    private static final Integer BATCH_SIZE = 500;

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
        try {
            return Duration.parse(period);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return Duration.ofMinutes(defaultValue);
    }


    public static Period buildTaskPeriod(String period) {
        return buildTaskPeriod(period, 1);
    }

    public static Period buildTaskPeriod(String period, int defaultValue) {
        if (ObjectUtils.isEmpty(period)) {
            return Period.ofDays(defaultValue);
        }
        try {
            return Period.parse(period);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return Period.ofDays(defaultValue);
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


    public static List<TaskSegment> list(String taskId, Integer start, Integer end, Integer spiltCount) {
        Integer tempEnd = 0;
        ArrayList<TaskSegment> taskSegements = new ArrayList<>();
        int i = 0;
        while (tempEnd < end) {
            tempEnd = start + spiltCount;
            if (tempEnd > end) {
                tempEnd = end;
            }

            taskSegements.add(
                    TaskSegment.builder()
                            .taskId(taskId)
                            .segmentId(++i)
                            .status(TaskStatusEnum.WORKING.getCode())
                            .startIndex(start)
                            .endIndex(tempEnd)
                            .build()

            );
            batchSaveBarrier(taskSegements, i);
            start = tempEnd + 1;
        }

        return taskSegements;
    }

    private static void batchSaveBarrier(ArrayList<TaskSegment> taskSegments, int i) {
        if (i % BATCH_SIZE == 0) {
            SpringUtil.publishEvent(new BatchSaveSegmentTaskEvent(taskSegments));
            taskSegments.clear();
        }
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

    public static List<TaskSegment> list(String taskId, List<?> dataList, Integer batchSize) {
        int totalCount = dataList.size() / batchSize;
        if (totalCount == 0) {
            ArrayList<TaskSegment> taskSegements = new ArrayList<>();
            taskSegements.add(TaskSegment.builder()
                    .taskId(taskId)
                    .segmentId(0)
                    .status(TaskStatusEnum.WORKING.getCode())
                    .snapshotValue(JsonUtil.toJson(dataList))
                    .build()
            );
            return taskSegements;
        }
        int leaveSize = dataList.size() % batchSize;
        if (leaveSize != 0) {
            totalCount++;
        }
        ArrayList<TaskSegment> objects = new ArrayList<>();
        for (int i = 0; i < totalCount; i++) {
            int from = i * batchSize;
            int to = i * batchSize + batchSize;
            if (i == totalCount - 1) {
                to = dataList.size();
            }
            List<?> subList = dataList.subList(from, to);
            TaskSegment build = TaskSegment.builder()
                    .taskId(taskId)
                    .segmentId(i + 1)
                    .status(TaskStatusEnum.WORKING.getCode())
                    .snapshotValue(JsonUtil.toJson(subList))
                    .build();
            objects.add(build);
            batchSaveBarrier(objects, i);
        }
        return objects;
    }


    public static List<TaskSegment> list(String taskId, LocalDateTime start, LocalDateTime end, TemporalAmount duration) {
        ArrayList<TaskSegment> objects = new ArrayList<>();
        LocalDateTime tempStart = start;
        LocalDateTime tempEnd = start;
        int i = 0;
        while (true) {
            tempEnd = tempStart.plus(duration);
            if (tempStart.plusNanos(1).isAfter(end)) {
                break;
            }
            TaskSegment build = TaskSegment.builder()
                    .taskId(taskId)
                    .segmentId(++i)
                    .status(TaskStatusEnum.WORKING.getCode())
                    .startTime(tempStart)
                    .endTime(tempEnd)
                    .build();

            objects.add(build);
            tempStart = tempEnd;
            batchSaveBarrier(objects, i);
        }
        return objects;
    }

    public static List<TimeSegment> list(LocalDateTime start, LocalDateTime end, TemporalAmount duration) {
        ArrayList<TimeSegment> objects = new ArrayList<>();
        LocalDateTime tempStart = start;
        LocalDateTime tempEnd = start;
        while (true) {
            tempEnd = tempStart.plus(duration);
            if (tempStart.plusNanos(1).isAfter(end)) {
                break;
            }

            objects.add(new TimeSegment(tempStart, tempEnd));
            tempStart = tempEnd;
        }
        return objects;
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
}
