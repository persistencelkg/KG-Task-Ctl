package org.kg.ctl.service;

import org.springframework.context.ApplicationEvent;

/**
 * Description: 批量生产快照任务事件，每到500条触发一次更新
 * Author: 李开广
 * Date: 2023/6/1 8:35 PM
 */
public class BatchSaveSegmentTaskEvent extends ApplicationEvent {

    /**
     * Create a new {@code ApplicationEvent}.
     *
     * @param source the object on which the event initially occurred or with
     *               which the event is associated (never {@code null})
     */
    public BatchSaveSegmentTaskEvent(Object source) {
        super(source);
    }
}
