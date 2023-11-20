package org.kg.ctl.service;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.springframework.util.ObjectUtils;

import java.util.List;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/5/23 3:22 PM
 */
public interface TaskService {

    List<TaskPo> listWorkingSnapshot(String taskId, Integer mode);

    void saveSnapshot(TaskPo taskPo);

    void updateTask(TaskPo taskPo);

    void deleteTask(String taskId);


}
