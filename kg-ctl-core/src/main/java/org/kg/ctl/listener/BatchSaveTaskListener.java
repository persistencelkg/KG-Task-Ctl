package org.kg.ctl.listener;

import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.manager.TaskHandler;
import org.kg.ctl.service.BatchSaveSegmentTaskEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/6/1 8:48 PM
 */
@Component
@Slf4j
public class BatchSaveTaskListener implements ApplicationListener<BatchSaveSegmentTaskEvent> {

    @Resource
    private TaskHandler taskHandler;

    @Override
    public void onApplicationEvent(BatchSaveSegmentTaskEvent event) {
        List<TaskSegment> source = (List<TaskSegment>) event.getSource();
        taskHandler.saveSegment(source);
        log.info("new batch segments has save  ----> task_id:{}",  source.get(0).getTaskId());
    }
}
