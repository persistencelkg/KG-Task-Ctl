package org.kg.ctl.manager;

import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.service.TaskSegmentService;
import org.kg.ctl.service.TaskService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.List;

/**
 * @description: 业务聚合
 * @author: 李开广
 * @date: 2023/5/23 3:58 PM
 */
@Service
public class TaskHandler {

    @Resource
    private TaskService taskService;
    @Resource
    private TaskSegmentService taskSegmentService;


    public List<TaskPo> getWorkingSnapShot(String taskId) {
        return taskService.listWorkingSnapshot(taskId);
    }

    public void saveOrUpdateSnapshot(TaskPo taskPo) {
        taskService.saveSnapshot(taskPo);
    }

    public void saveSegment(List<TaskSegment> taskSegmentList) {
        taskSegmentService.insertTaskSegments(taskSegmentList);
    }

    public void updateTask(TaskPo taskPo) {
        taskService.updateTask(taskPo);
    }

    public void updateTaskSegment(TaskSegment taskSegement) {
        taskSegmentService.updateByTaskId(taskSegement);
    }


    public List<TaskSegment> listSegmentWithOrder(String taskId) {
        List<TaskSegment> taskSegments = taskSegmentService.listSegmentWithTaskId(taskId);
        if (ObjectUtils.isEmpty(taskSegments)) {
            return null;
        }
        // 获取自己的内容
        return taskSegments;
    }

    @Transactional(rollbackFor = Exception.class)
    public void deleteTaskWithSegment(String taskId) {
        taskService.deleteTask(taskId);
        taskSegmentService.deleteTaskSegments(taskId);
    }
}
