package org.kg.ctl.service;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.mapper.TaskSegmentMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.List;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/5/23 3:24 PM
 */
@Service
public class TaskSegmentServiceImpl extends ServiceImpl<TaskSegmentMapper, TaskSegment> implements TaskSegmentService ,QueryBaseService<TaskSegment> {
    @Override
    public List<TaskSegment> listSegmentWithTaskId(Integer taskId) {
        LambdaQueryWrapper<TaskSegment> sqlQuery = this.sqlQuery();
        sqlQuery.eq(!ObjectUtils.isEmpty(taskId), TaskSegment::getTaskId, taskId)
                .eq(TaskSegment::getStatus, TaskStatusEnum.WORKING.getCode());
        return this.list(sqlQuery);
    }

    @Override
    public void updateByTaskId(TaskSegment record) {
        LambdaUpdateWrapper<TaskSegment> updateWrapper = this.sqlUpdate();
        updateWrapper.eq(!ObjectUtils.isEmpty(record.getTaskId()), TaskSegment::getTaskId, record.getTaskId())
                     .eq(!ObjectUtils.isEmpty(record.getSegmentId()), TaskSegment::getSegmentId, record.getSegmentId());
        this.update(record, updateWrapper);
    }

    @Override
    public void insertTaskSegments(List<TaskSegment> list) {
         super.saveBatch(list);
    }

    @Override
    public void deleteTaskSegments(String taskId) {
        LambdaQueryWrapper<TaskSegment> taskPoLambdaQueryWrapper = this.sqlQuery();
        taskPoLambdaQueryWrapper.eq(!ObjectUtils.isEmpty(taskId), TaskSegment::getTaskId, taskId);
        // TODO 限流
        super.remove(taskPoLambdaQueryWrapper);
    }

    @Override
    public TaskSegment listLastSegment(Integer taskId) {
        LambdaQueryWrapper<TaskSegment> query = this.sqlQuery();
        query.eq(!ObjectUtils.isEmpty(taskId), TaskSegment::getTaskId, taskId)
                                .orderByDesc(TaskSegment::getUpdateTime);
        query.last("limit 1");
        return this.getOne(query);
    }
}
