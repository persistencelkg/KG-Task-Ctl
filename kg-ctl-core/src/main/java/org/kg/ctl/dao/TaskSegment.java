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

    private Integer segmentId;

    private String snapshotValue;

    private Integer status;

    /**
     * biz id  子任务正在执行的起点
     */
    private Integer startIndex;

    private Integer endIndex;


    private LocalDateTime startTime;

    private LocalDateTime endTime;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

}