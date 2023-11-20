package org.kg.ctl.service;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.mapper.TaskMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.List;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/5/23 3:18 PM
 */
@Service
public class TaskServiceImpl extends ServiceImpl<TaskMapper, TaskPo> implements  TaskService,QueryBaseService<TaskPo> {

    @Resource
    private TaskSegmentService taskSegmentService;

    @Override
    public List<TaskPo> listWorkingSnapshot(String taskId, Integer mode) {
        LambdaQueryWrapper<TaskPo> sqlQuery = this.sqlQuery();
        sqlQuery.eq(!ObjectUtils.isEmpty(taskId), TaskPo::getTaskId, taskId);
        sqlQuery.eq(!ObjectUtils.isEmpty(mode), TaskPo::getMode, mode);
        return this.list(sqlQuery);
    }

    @Override
    public void saveSnapshot(TaskPo taskPo) {
        LambdaUpdateWrapper<TaskPo> eq = this.sqlUpdate()
                .eq(TaskPo::getTaskId, taskPo.getTaskId())
                .eq(TaskPo::getMode, taskPo.getMode());
        this.saveOrUpdate(taskPo, eq);
    }

    @Override
    public void updateTask(TaskPo taskPo) {
        super.updateById(taskPo);
    }


    @Override
    public void deleteTask(String taskId) {
        LambdaQueryWrapper<TaskPo> taskPoLambdaQueryWrapper = this.sqlQuery();
        taskPoLambdaQueryWrapper.eq(!ObjectUtils.isEmpty(taskId), TaskPo::getTaskId, taskId);
        this.remove(taskPoLambdaQueryWrapper);
    }




}
