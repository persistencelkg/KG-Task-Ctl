package org.kg.ctl.dao;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kg.ctl.util.DateTimeUtil;

import java.time.LocalDateTime;
import java.util.Objects;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskSegment {

    private String mode;


    /**
     * 当前批次的id，暂无使用
     */
    private Integer segmentId;

    /**
     * BIZ_ID : 子数据集合【起始索引】
     * DIVIDE_TABLE: SUFFIX of INDEX
     */
    private String snapshotValue;

    private Integer status;

    /**
     * biz id  子任务正在执行的起点
     */
    private Integer startIndex;

    private Integer endIndex;

    /**
     * 执行任务间隔的开始时间
     */
    private LocalDateTime startTime;

    private LocalDateTime endTime;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;


    public String getTimeRange() {
        if (Objects.nonNull(startTime) && Objects.nonNull(endTime)) {
            return DateTimeUtil.format(startTime) + "-->" + DateTimeUtil.format(endTime);
        }
        return "no time";
    }
}