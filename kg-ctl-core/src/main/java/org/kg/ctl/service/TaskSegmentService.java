package org.kg.ctl.service;

import org.kg.ctl.dao.TaskSegment;

import java.util.List;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/5/23 3:23 PM
 */
public interface TaskSegmentService {

    List<TaskSegment> listSegmentWithTaskId(String taskId);

    void updateByTaskId(TaskSegment record);

    void insertTaskSegments(List<TaskSegment> list);

    void deleteTaskSegments(String taskId);
}
