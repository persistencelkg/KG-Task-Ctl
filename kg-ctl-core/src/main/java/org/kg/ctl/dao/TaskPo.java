package org.kg.ctl.dao;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.enums.DataSourceEnum;
import org.kg.ctl.dao.enums.InsertModeEnum;
import org.kg.ctl.dao.enums.TaskModeEnum;
import org.kg.ctl.util.DateTimeUtil;
import org.kg.ctl.util.JsonUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Objects;

/**
 * Author 李开广
 * Date 2023/4/11 5:22 下午
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TaskPo {

    private String taskId;

    /**
     * 任务执行纬度
     */
    private Integer taskDimension;
    /**
     * segment 总信息
     */
    private String initialSnapShot;

    private LocalDateTime createTime;

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


        private LocalDateTime startTime;
        private LocalDateTime endTime;
        /**
         * ps: 请保证你的target time 是有索引的
         */
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
        private String mode;

        private String insertMode;

        /**
         * 同步间隔:default：PT1M
         * 一次扫描时间间隔
         */
        private String syncInterval;

        /**
         * 同步周期：T-1D T+1D T-1H TOD
         */
        private String syncPeriod;

        private String countDownInterval;

        public boolean isIncrementSync() {
            return TaskModeEnum.isIncrementSync(mode);
        }

        public boolean isProcessUniqueData(){
            return !CollectionUtils.isEmpty(dataList);
        }

        public void checkValid() {
            // 需要targetTime
            Assert.isTrue(Objects.nonNull(targetTime), "you choose time range sync, but not supply base on which time field");
            // 间隔应该不超过1天
            Duration duration = TaskUtil.buildTaskDuration(getSyncInterval());
            Assert.isTrue(duration.toHours() < 24, "task assign sync interval too large，exist performance issues");
            TaskModeEnum modeEnum = TaskModeEnum.getInstance(mode);
            Assert.isTrue(Objects.nonNull(modeEnum), "only support sync mode: " + mode);
            if (TaskModeEnum.isIncrementSync(mode)) {
                Assert.isTrue(targetTime.contains("update"), "you choose incr time sync, should use like `update_time`");
                Assert.isTrue(DateTimeUtil.isValid(getSyncPeriod()), "invalid sync period:" + getSyncPeriod() +", example: T-1、 T+1D");
                if (!ObjectUtils.isEmpty(getCountDownInterval())) {
                    LocalDateTime plus = LocalDateTime.now().plus(TaskUtil.buildTaskPeriod(getCountDownInterval()));
                    Assert.isTrue(plus.isBefore(LocalDateTime.now()), "increment sync current before interval not valid, no data happen future");
                }
            } else {
                Assert.isTrue(targetTime.contains("create"), "you choose full_sync time sync, should use like `create_time`");
                Assert.isTrue(Objects.nonNull(getStartTime())
                        && Objects.nonNull(getEndTime())
                        && getStartTime().isBefore(getEndTime()), "you choose `full_sync`, but not set a valid time range");
            }
        }

        public TemporalAmount convertToPeriod() {
            if (!syncPeriod.endsWith("D")) {
                syncPeriod += "D";
            }
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
            build.insertMode(param.getInsertMode());
            build.countDownInterval(param.getBeforeNowInterval());
            if (Objects.nonNull(param.getStartTime()) && Objects.nonNull(param.getEndTime())) {
                build.startTime(DateTimeUtil.parse(param.getStartTime())).endTime(DateTimeUtil.parse(param.getEndTime()));
            }
            return build.build();
        }

        public String getTimeRange() {
            if (Objects.nonNull(startTime) && Objects.nonNull(endTime)) {
                return DateTimeUtil.format(startTime) + "-->" + DateTimeUtil.format(endTime);
            }
            return "no time";
        }


        public boolean isDivideTable() {
            return Objects.nonNull(getTableStart()) && Objects.nonNull(getTableEnd());
        }


        public boolean isInsertCover() {
            return Objects.equals(InsertModeEnum.COVER.getMode(), this.insertMode);
        }

        private Integer getDivideTableBatchSize() {
            if (isDivideTable()) {
                return tableEnd + 1;
            }
            return -1;
        }

        public int isValidDivideTable() {
            Integer number = getDivideTableBatchSize();
            // 如果输入小于等于0或者不是2的幂次方，返回-1表示无效
            if (number <= 0 || (number & (number - 1)) != 0) {
                return -1;
            }
            int exponent = 0;
            while (number > 1) {
                number >>= 1;
                exponent++;
            }
            return exponent;
        }

        public static InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList) {
            return getTask(ds, dataList, null, null);
        }

        public static InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList, String index) {
            return getTask(ds, dataList, index, null);
        }

        public static InitialSnapShot getTask(DataSourceEnum ds, String index, String startTime, String endTime) {
            return getTask(ds, null, index, startTime, endTime);
        }

        public static InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList, String index,
                                                     String startTime, String endTime) {
            return getTask(ds, dataList, index,
                    String.join(JobConstants.LINE, startTime, endTime));
        }

        public static InitialSnapShot getTask(DataSourceEnum ds, Collection<?> dataList, Integer startId, Integer endId,
                                                     String startTime, String endTime) {
            return getTask(ds, dataList,
                    String.join(JobConstants.LINE, startId.toString(), endId.toString()),
                    String.join(JobConstants.LINE, startTime, endTime));
        }

        public static InitialSnapShot getTask(DataSourceEnum ds, @Nullable Collection<?> dataList,
                                                     @Nullable String index, @Nullable String timeRange) {
            InitialSnapShot baskTaskPo = new InitialSnapShot();
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
        String str = "{\"sync_period\":\"T-1D\",\"start_time\":\"2023-08-19 23:11:11\",\"end_time\":null,\"data_list\":[\"2\",\"33\"],\"min_id\":null,\"max_id\":null,\"index\":\"22A\",\"ds\":null}";
        InitialSnapShot bean = JsonUtil.toBean(str, InitialSnapShot.class);
        InitialSnapShot mysql = InitialSnapShot.getTask(DataSourceEnum.ES, null, "2022-01-01 00:00:00->2022-03-03 00:00:00");
        System.out.println(DateTimeUtil.format(LocalDateTime.now().plus(bean.convertToPeriod())));

    }
}
