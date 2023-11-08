package org.kg.ctl.dao;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("task_segment")
public class TaskSegment {

    @TableId(type = IdType.AUTO)
    private Integer id;

    private String taskId;

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

}