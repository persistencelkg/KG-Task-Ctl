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
import org.kg.ctl.dao.enums.TaskTimeSplitEnum;
import org.kg.ctl.util.DateTimeUtil;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

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

        private Integer minId;
        private Integer maxId;
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
         * 同步纬度，当选择时间纬度才会使用
         * such as： year, month，day
         */
        private String syncDimension;
        /**
         * 同步间隔
         */
        private Integer syncInterval;


        public static InitialSnapShot convertToSnapShot(TaskExecuteParam param) {
            InitialSnapShotBuilder build = InitialSnapShot.builder()

                    .dataList(param.getDataList())
                    .targetTime(param.getTargetTime())
                    .targetBizId(param.getTargetBizId())
                    .index(param.getTablePreFix());
            if (!StringUtils.isEmpty(param.getTableRange())) {
                String tableRange = param.getTableRange();
                String[] split = tableRange.split(JobConstants.LINE);
                build.minId(Integer.valueOf(split[0])).maxId(Integer.valueOf(split[1]));
                build.syncDimension(param.getSyncDimension());
                build.syncInterval(param.getSyncInterval());
            }
            if (Objects.nonNull(param.getStartTime()) && Objects.nonNull(param.getEndTime())) {
                build.startTime(DateTimeUtil.parse(param.getStartTime())).endTime(DateTimeUtil.parse(param.getEndTime()));
            }
            return build.build();
        }


        public boolean isDivideTable() {
            return Objects.isNull(getMinId()) || Objects.isNull(getMaxId());
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
        String str = "{\"mode\":1,\"start_time\":\"2023-08-19T23:11::11\",\"end_time\":null,\"data_list\":['2','33'],\"min_id\":null,\"max_id\":null,\"index\":\"22A\",\"ds\":null}";
        System.out.println(str);

        ArrayList<Object> objects = new ArrayList<>();

        TaskPo.InitialSnapShot mysql = InitialSnapShot.getTask(DataSourceEnum.ES, null, "2022-01-01 00:00:00->2022-03-03 00:00:00");
        System.out.println(mysql);
    }
}
