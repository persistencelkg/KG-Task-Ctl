package org.kg.ctl.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.springframework.util.ObjectUtils;

import java.util.List;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/5/23 3:23 PM
 */
public interface TaskSegmentService {

    List<TaskSegment> listSegmentWithTaskId(Integer taskId);

    void updateByTaskId(TaskSegment record);

    void insertTaskSegments(List<TaskSegment> list);

    void deleteTaskSegments(Integer taskId);

    void deleteTaskSegments(List<Integer> batchTaskIds);

    TaskSegment listLastSegment(Integer taskId);

}
