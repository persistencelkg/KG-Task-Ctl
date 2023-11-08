package org.kg.ctl.dao;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.enums.DataSourceEnum;
import org.kg.ctl.dao.enums.TaskDimensionEnum;
import org.kg.ctl.util.DateTimeUtil;
import org.kg.ctl.util.JsonUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.*;

/**
 * @author likaiguang
 * @date 2023/4/11 5:22 下午
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@TableName("task")
public class TaskPo {

    @TableId(type = IdType.AUTO)
    private Integer id;

    private String taskId;

    /**
     * 任务整体运行状态
     */
    private Integer taskStatus;

    /**
     * 任务执行纬度
     */
    private Integer taskDimension;
    /**
     * segment 总信息
     */
    private String initialSnapShot;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer mode;


    /**
     * 静态初始快照，一旦确定 一定得所有的子任务完成才能更改
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
    public static class InitialSnapShot {

        public static final int INCR_SYNC = 0;
        public static final int ALL_INF_SYNC = 1;



        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private String targetTime;

        private Collection<?> dataList;
        private String targetBizId;

        private Integer tableStart;
        private Integer tableEnd;

        /**
         * es index为索引名
         * mysql index为表前缀
         */
        private String index;
        private String ds;

        /**
         * 同步模式
         */
        private Integer mode;


        /**
         * 同步间隔
         */
        private String syncInterval;

        /**
         * 同步周期：T-1D T+1D T-1H TOD
         */
        private String syncPeriod;

        private String countDownInterval;

//        private Set<String> checkedFields = new HashSet<>();

        public boolean isIncrementSync() {
            return mode == TaskPo.InitialSnapShot.INCR_SYNC;
        }


        public void checkValid(TaskDimensionEnum instance) {
            if (Objects.equals(instance, TaskDimensionEnum.TIME_RANGE)) {
                // 间隔应该不超过1天
                try {
                    Assert.notNull(LocalDateTime.now().plus(TaskUtil.buildTaskDuration(getSyncInterval())), "task assign sync interval not valid");
                } catch (Exception e) {
                    Assert.isTrue(false, "task assign sync interval not valid or not unreasonable: " +  e.getMessage());
                }
                Assert.isTrue(getMode() == TaskPo.InitialSnapShot.INCR_SYNC || getMode() == TaskPo.InitialSnapShot.ALL_INF_SYNC, "only support sync mode: [0,1]");
                if (isIncrementSync()){
                    try {
                        Assert.notNull(getSyncPeriod(), "task assign sync period not empty");
                        Assert.isTrue(syncPeriod.endsWith("D"), "invalid assign sync period：" + syncPeriod);
                        LocalDateTime plus = LocalDateTime.now().plus(TaskUtil.buildTaskPeriod(getSyncPeriod()));
                        Assert.isTrue(plus.isBefore(LocalDateTime.now()), "task assign sync period not valid, no data happen future");
                    } catch (Exception e) {
                        Assert.isTrue(false, "task assign sync period not valid: " +  e.getMessage());
                    }
                    if (!ObjectUtils.isEmpty(getCountDownInterval())) {
                        try {
                            LocalDateTime plus = LocalDateTime.now().plus(TaskUtil.buildTaskPeriod(getCountDownInterval()));
                            Assert.isTrue(plus.isBefore(LocalDateTime.now()), "increment sync current before interval not valid, no data happen future");
                        } catch (Exception e) {
                            Assert.isTrue(false, "increment sync current before interval not valid or not unreasonable: " +  e.getMessage());
                        }

                    }
                } else {
                    Assert.isTrue(Objects.nonNull(getStartTime())
                            && Objects.nonNull(getEndTime())
                            && getStartTime().isBefore(getEndTime()), "you choose `all-in sync`, but not set a valid time range");
                }
            }
        }

        public TemporalAmount convertToPeriod() {
            String canBeConvert = syncPeriod.contains("D") ? syncPeriod.replace("T", "P") : "P" + syncPeriod;
            return TaskUtil.buildDurationPeriod(canBeConvert);
        }

        public static InitialSnapShot convertToSnapShot(TaskExecuteParam param) {
            InitialSnapShotBuilder build = InitialSnapShot.builder()

                    .dataList(param.getDataList())
                    .targetTime(param.getTargetTime())
                    .targetBizId(param.getTargetBizId())
                    .index(param.getTablePreFix());
            if (!StringUtils.isEmpty(param.getTableRange())) {
                String tableRange = param.getTableRange();
                String[] split = tableRange.split(JobConstants.LINE);
                build.tableStart(Integer.valueOf(split[0])).tableEnd(Integer.valueOf(split[1]));
            }
            build.index(param.getTablePreFix());
            build.syncInterval(param.getSyncInterval());
            build.syncPeriod(param.getSyncPeriod());
            build.mode(param.getMode());
            build.countDownInterval(param.getBeforeNowInterval());
            if (Objects.nonNull(param.getStartTime()) && Objects.nonNull(param.getEndTime())) {
                build.startTime(DateTimeUtil.parse(param.getStartTime())).endTime(DateTimeUtil.parse(param.getEndTime()));
            }
            return build.build();
        }


        public boolean isDivideTable() {
            return Objects.nonNull(getTableStart()) && Objects.nonNull(getTableEnd());
        }

        public Integer getTotalCount() {
            if (isDivideTable()) {
                return tableEnd + 1;
            }
            return -1;
        }


        public static TaskPo.InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList) {
            return getTask(ds, dataList, null, null);
        }

        public static TaskPo.InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList, String index) {
            return getTask(ds, dataList, index, null);
        }

        public static TaskPo.InitialSnapShot getTask(DataSourceEnum ds, String index, String startTime, String endTime) {
            return getTask(ds, null, index, startTime, endTime);
        }

        public static TaskPo.InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList, String index,
                                                     String startTime, String endTime) {
            return getTask(ds, dataList, index,
                    String.join(JobConstants.LINE, startTime, endTime));
        }

        public static TaskPo.InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList, Integer startId, Integer endId,
                                                     String startTime, String endTime) {
            return getTask(ds, dataList,
                    String.join(JobConstants.LINE, startId.toString(), endId.toString()),
                    String.join(JobConstants.LINE, startTime, endTime));
        }

        public static TaskPo.InitialSnapShot getTask(DataSourceEnum ds, @Nullable Collection<?> dataList,
                                                     @Nullable String index, @Nullable String timeRange) {
            TaskPo.InitialSnapShot baskTaskPo = new TaskPo.InitialSnapShot();
            if (!ObjectUtils.isEmpty(dataList)) {
                baskTaskPo.setDataList(dataList);
            }
            Assert.isTrue(!ObjectUtils.isEmpty(ds), "data source cant not null");
            ds.checkAndInit(index, baskTaskPo);
            if (ObjectUtils.isEmpty(ds)) {
                baskTaskPo.setDs(DataSourceEnum.MYSQL.getDs());
            } else {
                baskTaskPo.setDs(ds.getDs());
            }
            String line = JobConstants.LINE;
            if (!ObjectUtils.isEmpty(timeRange) && timeRange.contains(line)) {
                String[] split = timeRange.split(line);
                baskTaskPo.setStartTime(DateTimeUtil.parse(split[0]));
                baskTaskPo.setEndTime(DateTimeUtil.parse(split[1]));
            }
            return baskTaskPo;
        }

    }


    public static void main(String[] args) {
        String str = "{\"sync_period\":\"T-1H\",\"start_time\":\"2023-08-19 23:11:11\",\"end_time\":null,\"data_list\":[\"2\",\"33\"],\"min_id\":null,\"max_id\":null,\"index\":\"22A\",\"ds\":null}";
        InitialSnapShot bean = JsonUtil.toBean(str, InitialSnapShot.class);
        TaskPo.InitialSnapShot mysql = InitialSnapShot.getTask(DataSourceEnum.ES, null, "2022-01-01 00:00:00->2022-03-03 00:00:00");
        System.out.println(DateTimeUtil.format(LocalDateTime.now().plus(bean.convertToPeriod())));
    }
}
