package org.kg.ctl.core;


import com.baomidou.mybatisplus.core.toolkit.StringPool;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskExecuteParam;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskDimensionEnum;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.manager.TaskHandler;
import org.kg.ctl.service.DingDingService;
import org.kg.ctl.service.TaskControlService;
import org.kg.ctl.service.TaskMachine;
import org.kg.ctl.util.JsonUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * TODO: 1.服务扩缩容，影响分片任务
 *       2.无xxl-job例如使用了Elastic-Job的分片规则如何确定？ 1)单机【无需考虑】 2)集群【是否可以基于Nacos拉取服务列表来自定义默认的调度策略】
 *
 * @author likaiguang
 * @date 2023/4/11 7:33 下午
 */
@Slf4j
public abstract class AbstractTaskDispatcher implements TaskMachine, TaskControlService, DingDingService {

    @Resource
    private TaskHandler taskHandler;

    @Value("${spring.application.name:''}")
    private String applicationName;

    @Value("${spring.profiles.active:local}")
    private String env;


    protected void runTask() {
        try {
            log.info("ct task start ---->");
            buildTask();
            log.info("ctl task finished ---->");
        } catch (Exception e) {
            log.error("ctl task error ---->", e);
        }
    }

    @Override
    public String getEnv() {
        return MessageFormat.format("【env:{0}】|【task:{1}】", env, getTaskId());
    }

    public boolean isNegative(Object num) {
        if (Objects.isNull(num)) {
            return false;
        }
        if (num instanceof Integer) {
            return (Integer) num > 0;
        }
        if (num instanceof Long) {
            return (Long) num > 0L;
        }
        return false;
    }

    protected abstract void checkValid(TaskPo.InitialSnapShot taskSnapShot);

    private TaskDimensionEnum prepareBuildTask(TaskPo.InitialSnapShot taskSnapShot) {
        Assert.notNull(taskSnapShot, "please generate global snapshot with TaskCtlGenerator.list(...)");
        TaskDimensionEnum instance = TaskDimensionEnum.getInstance(taskSnapShot);
        // 基础参数校验
        Assert.notNull(instance, "you not point at task split dimension such as:" + Arrays.toString(TaskDimensionEnum.values()));
        Assert.isTrue(isNegative(this.getSleepTime()), "each split task must have valid milliseconds sleep time");
        Assert.isTrue(isNegative(this.getBatchSize()) && isNegative(this.getConcurrentThreadCount()), "each split task must have valid batch size");
        Assert.notNull(this.getTaskSplitDuration(), "you choose time range to split task， must point at duration ");
        // 定制参数校验
        checkValid(taskSnapShot);
        return instance;

    }

    protected String getTaskId() {
        return String.join(StringPool.UNDERSCORE, applicationName, this.getClass().getSimpleName());
    }

    private void buildTask() {
        String simpleName = getTaskId();
        // 获取进行中的快照
        if (existWorkingSnapshot(simpleName)) {
            return;
        }
        TaskPo taskJob;
        String param = getParam();
        TaskExecuteParam bean = JsonUtil.toBean(param, TaskExecuteParam.class);
        TaskPo.InitialSnapShot taskSnapShot = instantiationSnapShot(bean);
        taskJob = new TaskPo();
        taskJob.setTaskId(simpleName);
        taskJob.setTaskStatus(TaskStatusEnum.DEFAULT.getCode());

        // 预校验
        TaskDimensionEnum instance = prepareBuildTask(taskSnapShot);
        taskJob.setInitialSnapShot(JsonUtil.toJson(taskSnapShot));
        taskJob.setTaskDimension(instance.getDimension());

        AbstractTaskDispatcher abstractTaskDispatcher = (AbstractTaskDispatcher) AopContext.currentProxy();
        List<TaskSegment> taskSegementList = abstractTaskDispatcher.saveSnapshot(simpleName, taskJob, taskSnapShot, instance);

        // 按机器实例切分
        List<TaskSegment> belongOwnTaskList = TaskUtil.list(taskSegementList, getIndex(), getTotalCount());
        // 执行任务
        doTask(taskSnapShot, belongOwnTaskList);
        // 尝试完结任务
        tryFinishJob(taskJob, taskSnapShot, taskSegementList.get(taskSegementList.size() - 1));
    }


    @Transactional(rollbackFor = Exception.class)
    public List<TaskSegment> saveSnapshot(String simpleName, TaskPo taskJob, TaskPo.InitialSnapShot taskSnapShot, TaskDimensionEnum instance) {
        // 保存全局快照
        taskHandler.saveSnapshot(taskJob);
        // 生产所有子任务
        List<TaskSegment> taskSegementList = splitTask(simpleName, taskSnapShot);
        taskHandler.saveSegment(taskSegementList);
        // 前置任务由BatchSaveTaskListener完成
        log.info("generate global snapshot:{} ---> build success, last batch task segment list size:{}", taskSnapShot, taskSegementList.size());
        // signal all
        taskJob.setTaskStatus(TaskStatusEnum.WORKING.getCode());
        taskHandler.updateTask(taskJob);
        return taskSegementList;
    }

    private boolean existWorkingSnapshot(String simpleName) {
        TaskPo taskJob = taskHandler.getWorkingSnapShot(simpleName);
        List<TaskSegment> segmentList;
        if (!ObjectUtils.isEmpty(taskJob)) {
            // 如果还没初始化完毕，等待下次调度
            if (!TaskStatusEnum.WORKING.getCode().equals(taskJob.getTaskStatus())) {
                log.warn("task snapshot generating now, please wait");
                // 理论上改日志每台机器只会出现1次，如果出现多次，说明保存子快照失败了
                return true;
            }
            TaskPo.InitialSnapShot initialSnapShot = JsonUtil.toBean(taskJob.getInitialSnapShot(), TaskPo.InitialSnapShot.class);
            if (Objects.isNull(initialSnapShot)) {
                log.error("task id:{}, initial snapshot damage, please manually repair", taskJob.getTaskId());
                return false;
            }
            // 获取所有快照
            segmentList = taskHandler.listSegmentWithOrder(taskJob.getTaskId());
            if (!ObjectUtils.isEmpty(segmentList)) {
                //获取属于自己操作部分
                List<TaskSegment> belongOwn = TaskUtil.list(segmentList, getIndex(), getTotalCount());
                if (!ObjectUtils.isEmpty(belongOwn)) {
                    List<TaskSegment> workingTaskSegment = belongOwn.stream().filter(ref -> TaskStatusEnum.WORKING.getCode().equals(ref.getStatus())).collect(Collectors.toList());
                    TaskSegment taskSegement;
                    if (ObjectUtils.isEmpty(workingTaskSegment)) {
                        //获取最后一个
                        taskSegement = belongOwn.get(belongOwn.size() - 1);
                    } else {
                        dingInfoLog(MessageFormat.format("读取快照个数：{0} 上次执行位置:{1}", workingTaskSegment.size(), workingTaskSegment.get(0)));
                        doTask(initialSnapShot, workingTaskSegment);
                        taskSegement = workingTaskSegment.get(workingTaskSegment.size() - 1);
                    }
                    tryFinishJob(taskJob, initialSnapShot, taskSegement);
                    return true;
                }
            } else {
                log.warn("you select snapshot but not split task，so do nothing");
                return true;
            }
        }
        return false;
    }

    private void tryFinishJob(TaskPo taskjob, TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegement) {
        if (!TaskStatusEnum.FINISHED.getCode().equals(taskSegement.getStatus())) {
            return;
        }
        boolean flag = judgeTaskFinish(initialSnapShot, taskSegement);
        if (flag) {
            dingErrorLog(MessageFormat.format("job:{0}，global task has finished", taskjob.getTaskId()));
            taskjob.setTaskStatus(TaskStatusEnum.FINISHED.getCode());
            taskHandler.updateTask(taskjob);
        } else {
            log.info("job{} segment:{} phase task has finished, global task not finish", taskjob.getTaskId(), taskSegement.getId());
        }
    }

    protected abstract boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegement);

    protected abstract List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot);


    private void doTask(TaskPo.InitialSnapShot initialSnapShot, List<TaskSegment> workingTaskSegment) {
        int concurrentThreadNum = Objects.nonNull(this.getConcurrentThreadCount()) ? this.getConcurrentThreadCount() : getDefaultThreadCount();
        // n个 任务 分配给 m个线程
        int batchCount = workingTaskSegment.size() / concurrentThreadNum;
        log.info("concurrentThreadNum size:{}, each thread execute task size:{}", concurrentThreadNum, batchCount);
        if (workingTaskSegment.size() % concurrentThreadNum != 0) {
            batchCount++;
        }
        int threadCount = concurrentThreadNum;
        ExecutorService executorService = executorService();
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < batchCount; i++) {
            if (i == batchCount - 1) {
                threadCount = workingTaskSegment.size() % concurrentThreadNum;
            }
            CountDownLatch countDownLatch = new CountDownLatch(threadCount);

            for (int j = 0; j < threadCount; j++) {
                TaskSegment taskSegement = workingTaskSegment.get(count.getAndIncrement());
                // 停止具体的子任务
                if (!isRun()) {
                    dingErrorLog(MessageFormat.format("快照：{0} 手动停止", taskSegement));
                    return;
                }
                try {
                    executorService.execute(() -> {
                        executeTask(taskSegement, doExecuteTask(initialSnapShot));
                        countDownLatch.countDown();
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                    countDownLatch.countDown();
                    dingErrorLog(MessageFormat.format("快照：{0} 执行出现异常：{1}", taskSegement, e));
                }

            }
            try {
                countDownLatch.await();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

    }

    protected abstract Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot);


    protected void executeTask(TaskSegment taskSegement, Function<TaskSegment, Boolean> function) {
        dingInfoLog(MessageFormat.format("开始执行快照：{0}", taskSegement));
        Boolean apply = function.apply(taskSegement);
        if (!apply) {
            return;
        }
        taskSegement.setStatus(TaskStatusEnum.FINISHED.getCode());
        taskHandler.updateTaskSegment(taskSegement);
        dingInfoLog(MessageFormat.format("快照：{0}执行已完成", taskSegement));
        try {
            TimeUnit.MILLISECONDS.sleep(getSleepTime());
        } catch (InterruptedException ignored) {
        }
    }


    /**
     * 创建初始化快照
     * 具体可以通过 TaskCtlGenerator#getTask实现
     *
     * @return 全局快照
     */
    protected TaskPo.InitialSnapShot instantiationSnapShot(TaskExecuteParam param) {
        return TaskPo.InitialSnapShot.convertToSnapShot(param);
    }


}
