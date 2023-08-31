package org.kg.ctl.service;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.mapper.TaskMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/5/23 3:18 PM
 */
@Service
public class TaskServiceImpl extends ServiceImpl<TaskMapper, TaskPo> implements  TaskService,QueryBaseService<TaskPo> {

    @Resource
    private TaskSegmentService taskSegmentService;

    @Override
    public TaskPo getWorkingSnapShot(String taskId) {
        LambdaQueryWrapper<TaskPo> sqlQuery = this.sqlQuery();
        sqlQuery.eq(!ObjectUtils.isEmpty(taskId), TaskPo::getTaskId, taskId)
                .ne(TaskPo::getTaskStatus, TaskStatusEnum.FINISHED.getCode());
        return this.getOne(sqlQuery);
    }

    @Override
    public void saveSnapshot(TaskPo taskPo) {
        this.save(taskPo);
    }

    @Override
    public void updateTask(TaskPo taskPo) {
        this.updateById(taskPo);
    }


    @Override
    public void deleteTask(String taskId) {
        LambdaQueryWrapper<TaskPo> taskPoLambdaQueryWrapper = this.sqlQuery();
        taskPoLambdaQueryWrapper.eq(!ObjectUtils.isEmpty(taskId), TaskPo::getTaskId, taskId);
        this.remove(taskPoLambdaQueryWrapper);
    }
}
