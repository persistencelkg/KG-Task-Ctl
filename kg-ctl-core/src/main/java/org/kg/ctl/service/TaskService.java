package org.kg.ctl.service;


import org.kg.ctl.dao.TaskPo;

import java.util.List;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/5/23 3:22 PM
 */
public interface TaskService{

    List<TaskPo> listWorkingSnapshot(String taskId);

    void saveSnapshot(TaskPo taskPo);

    void updateTask(TaskPo taskPo);

    void deleteTask(String taskId);
}
