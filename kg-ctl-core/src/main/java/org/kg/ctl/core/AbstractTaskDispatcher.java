package org.kg.ctl.core;


import com.baomidou.mybatisplus.core.toolkit.StringPool;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskExecuteParam;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskDimensionEnum;
import org.kg.ctl.dao.enums.TaskModeEnum;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.manager.TaskHandler;
import org.kg.ctl.service.DingDingService;
import org.kg.ctl.service.TaskControlService;
import org.kg.ctl.service.TaskMachine;
import org.kg.ctl.util.DateTimeUtil;
import org.kg.ctl.util.JsonUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


/**
 * TODO: 1.如何服务扩缩容，影响分片任务
 *       2.无xxl-job例如使用了Elastic-Job的分片规则如何确定？
 *         1)单机【无需考虑】 2)集群【可以基于Nacos拉取服务列表来自定义默认的调度策略】
 *       3.分布式调度任务时，不要存储IP，应该以固定InstanceName代替，即只考虑参与任务的机器个数，而不应比较具体的数据， 一旦指定分布式配置，需要给出一个开关进行线程适配。
 *
 * @author likaiguang
 * @date 2023/4/11 7:33 下午
 */
@Slf4j
public abstract class AbstractTaskDispatcher implements TaskMachine, TaskControlService, DingDingService {

    @Resource
    @Getter
    private TaskHandler taskHandler;

    @Value("${spring.application.name:''}")
    private String applicationName;

    @Value("${spring.profiles.active:local}")
    private String env;


    protected void runTask() {
        try {

            log.info("ctl task start ---->");
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
        // config check
        Assert.isTrue(isNegative(this.getSleepTime()), "each split task must have valid milliseconds sleep time");
        Assert.isTrue(isNegative(this.getBatchSize()) && isNegative(this.getConcurrentThreadCount()), "each split task must have valid batch size");
        // param check
        Assert.notNull(taskSnapShot, "please generate global snapshot with TaskCtlGenerator.list(...)");
        TaskDimensionEnum instance = TaskDimensionEnum.getInstance(taskSnapShot);
        // 基础参数校验
        Assert.notNull(instance, "you not point at task split dimension such as:" + Arrays.toString(TaskDimensionEnum.values()));
        taskSnapShot.checkValid(instance);
        // param custom check
        checkValid(taskSnapShot);
        return instance;

    }

    protected String getTaskId() {
        return String.join(StringPool.UNDERSCORE, applicationName, this.getClass().getSimpleName());
    }

    private void buildTask() {
        String simpleName = getTaskId();
        if (isRun()) {
            log.warn("ctl task:{} current has stop，please reopen", simpleName);
            return;
        }
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
        taskJob.setTaskStatus(TaskStatusEnum.WORKING.getCode());
        taskJob.setInitialSnapShot(JsonUtil.toJson(taskSnapShot));
        taskJob.setMode(taskSnapShot.getMode());
        // 预校验
        TaskDimensionEnum instance = prepareBuildTask(taskSnapShot);
        taskJob.setTaskDimension(instance.getDimension());
        // 保存全局快照
        taskHandler.saveOrUpdateSnapshot(taskJob);
        log.info("generate global snapshot:{} ---> build success, start split segments and run task", taskSnapShot);
        splitAndRunTaskWithDivideTable(taskJob, taskSnapShot);
    }

    // initJob -> parseParam -> adaptMode
    // prepareJob -> getJOb -> doGetJob -> getProcessingTask  ---
    // validateJob -> commonCheck -> customCheck                |
    // createTask -> doCreateTask【splitTask】                  |
    // executeTask;                               <------------
    // finishJob


    private void processTask() {

    }

    public static int powerOfTwoExponent(int number) {
        // 如果输入小于等于0或者不是2的幂次方，返回-1表示无效
        if (number <= 0 || (number & (number - 1)) != 0) {
            return -1;
        }
        int exponent = 0;
        while (number > 1) {
            number >>= 1;
            exponent++;
        }
        return exponent;
    }


    //    @Transactional(rollbackFor = Exception.class)
    public boolean splitAndRunTaskWithDivideTable(TaskPo taskJob, TaskPo.InitialSnapShot taskSnapShot) {

        AtomicReference<TaskSegment> taskSegment = new AtomicReference<>();
        // 是否是分表
        if (taskSnapShot.isDivideTable()) {
            ExecutorService executorService = executorService();
            int batchDivideTable = powerOfTwoExponent(taskSnapShot.getTotalCount());
            if (batchDivideTable > 0) {
                int tempEnd = 0;
                int end = taskSnapShot.getTableEnd();
                int start = taskSnapShot.getTableStart();
                while (tempEnd < end) {
                    tempEnd = start + batchDivideTable;
                    if (tempEnd > end) {
                        tempEnd = end;
                    }
                    CountDownLatch countDownLatch = new CountDownLatch(tempEnd - start + 1);
                    for (int i = start; i <= tempEnd; i++) {
                        int finalI = i;

                        executorService.execute(() -> {
                            if (!isRun()) {
                                return;
                            }
                            String currentTableName = taskSnapShot.getIndex() + finalI;
                            taskSegment.set(splitAndRunTask(taskJob.getTaskId(), taskSnapShot, currentTableName));
                            countDownLatch.countDown();
                        });
                    }
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    start = tempEnd + 1;
                }
                return true;
            }
        }
        // 直接执行
        taskSegment.set(splitAndRunTask(taskJob.getTaskId(), taskSnapShot, null));
        return tryFinishJob(taskJob, taskSnapShot, taskSegment.get());
    }

    private boolean existWorkingSnapshot(String simpleName) {
        List<TaskPo> taskJob = taskHandler.getWorkingSnapShot(simpleName);
        if (CollectionUtils.isEmpty(taskJob)) {
            return false;
        }
        if (taskJob.size() > 2) {
            throw new IllegalArgumentException("task:[ " + simpleName + "] exist invalid job count: " + taskJob.size() + "，please check");
        }
        taskJob.sort(Comparator.comparingInt(TaskPo::getMode));
        boolean last = false;
        for (TaskPo taskPo : taskJob) {
            log.info("task：{} current mode: {}, status:{}", simpleName,
                    TaskModeEnum.getInstance(taskPo.getMode()).getMode(),
                    TaskStatusEnum.getInstance(taskPo.getTaskStatus()).getCode());
            last = processWorkingSnapshot(taskPo);
        }
        return last;
    }

    private boolean processWorkingSnapshot(TaskPo taskJob) {
        String simpleName = taskJob.getTaskId();
        List<TaskSegment> segmentList;
        if (ObjectUtils.isEmpty(taskJob)) {
            return false;
        }
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
        if (initialSnapShot.isIncrementSync()) {
            initIncrementSync(initialSnapShot);
            log.info("task:{} 开启新的一轮增量同步:{}", simpleName, DateTimeUtil.format(LocalDateTime.now()));
            return splitAndRunTaskWithDivideTable(taskJob, initialSnapShot);
        }
        // 获取所有快照
        segmentList = taskHandler.listSegmentWithOrder(taskJob.getTaskId());
        if (!ObjectUtils.isEmpty(segmentList)) {
            dynamicExecuteTask(segmentList, initialSnapShot);
            return tryFinishJob(taskJob, initialSnapShot, segmentList.get(segmentList.size() - 1));
            //获取属于自己操作部分
//            List<TaskSegment> belongOwn = TaskUtil.list(segmentList, getIndex(), getTotalCount());
//            if (!ObjectUtils.isEmpty(belongOwn)) {
//                List<TaskSegment> workingTaskSegment = belongOwn.stream().filter(ref -> TaskStatusEnum.WORKING.getCode().equals(ref.getStatus())).collect(Collectors.toList());
//                TaskSegment taskSegement;
//                if (ObjectUtils.isEmpty(workingTaskSegment)) {
//                    //获取最后一个
//                    taskSegement = belongOwn.get(belongOwn.size() - 1);
//                } else {
//                    dingInfoLog(MessageFormat.format("读取快照个数：{0} 上次执行位置:{1}", workingTaskSegment.size(), workingTaskSegment.get(0)));
//
//                    taskSegement = workingTaskSegment.get(workingTaskSegment.size() - 1);
//                }
//            }
        }
        log.warn("you select snapshot but not split task，so do nothing");
        return true;
    }

    private boolean tryFinishJob(TaskPo taskJob, TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        if (Objects.isNull(taskSegment)) {
            // TODO 给出结束标准
            log.error("task：{} maybe has stop, please check finish status", taskJob.getTaskId());
            return false;
        }
        if (!TaskStatusEnum.FINISHED.getCode().equals(taskSegment.getStatus())) {
            return false;
        }
        boolean flag = judgeTaskFinish(initialSnapShot, taskSegment);
        if (flag) {
            dingErrorLog(MessageFormat.format("job:{0}，global task has finished", taskJob.getTaskId()));
            taskJob.setTaskStatus(TaskStatusEnum.FINISHED.getCode());
            taskHandler.updateTask(taskJob);
        } else {
            log.info("job{} segment:{} phase task has finished, global task not finish", taskJob.getTaskId(), taskSegment.getId());
        }
        return flag;
    }

    protected abstract boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment);


    /// TODO 下一次怎么开始
    private TaskSegment splitAndRunTask(String taskId, TaskPo.InitialSnapShot initialSnapShot, String tableId) {
        TemporalAmount duration = TaskUtil.buildTaskDuration(initialSnapShot.getSyncInterval());
        ArrayList<TaskSegment> objects = new ArrayList<>();
        LocalDateTime tempStart = initialSnapShot.getStartTime();
        LocalDateTime tempEnd;
        TaskSegment lastSegment = null;
        int i = 0;
        while (isRun()) {
            tempEnd = tempStart.plus(duration);
            if (tempStart.plusNanos(1).isAfter(initialSnapShot.getEndTime())) {
                break;
            }

            TaskSegment build = TaskSegment.builder()
                    .taskId(taskId)
                    .segmentId(++i)
                    .status(TaskStatusEnum.WORKING.getCode())
                    .startTime(tempStart)
                    .endTime(tempEnd)
                    .build();
            if (Objects.nonNull(tableId)) {
                build.setSnapshotValue(tableId);
            }
            objects.add(build);
            if (objects.size() % getConcurrentThreadCount() == 0) {
                getTaskHandler().saveSegment(objects);
                dynamicExecuteTask(objects, initialSnapShot);
                lastSegment = objects.get(objects.size() - 1);
                objects.clear();
            }
            tempStart = tempEnd;
        }
        if (objects.size() > 0) {
            getTaskHandler().saveSegment(objects);
            dynamicExecuteTask(objects, initialSnapShot);
            lastSegment = objects.get(objects.size() - 1);
        }
        return lastSegment;
    }





    protected void dynamicExecuteTask(List<TaskSegment> workingTaskSegment, TaskPo.InitialSnapShot initialSnapShot) {
        int n = getConcurrentThreadCount();
        if (initialSnapShot.isDivideTable()) {
            for (int i = 0; i < n; i++) {
                TaskSegment taskSegment = workingTaskSegment.get(i);
                executeTask(taskSegment, doExecuteTask(initialSnapShot));
            }
            return;
        }
        // 分表以表纬度作为线程， 未分表以线程数为主
        ExecutorService executorService = executorService();
        CountDownLatch countDownLatch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            TaskSegment taskSegment = workingTaskSegment.get(i);
            try {
                executorService.execute(() -> {
                    executeTask(taskSegment, doExecuteTask(initialSnapShot));
                    countDownLatch.countDown();
                });
            } catch (Exception e) {
                e.printStackTrace();
                dingErrorLog(MessageFormat.format("快照：{0} 执行出现异常：{1}", taskSegment, e));
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

    }

//    private void doTask(TaskPo.InitialSnapShot initialSnapShot, List<TaskSegment> workingTaskSegment) {
//        int concurrentThreadNum = Objects.nonNull(this.getConcurrentThreadCount()) ? this.getConcurrentThreadCount() : getDefaultThreadCount();
//        // n个 任务 分配给 m个线程
//        int batchCount = workingTaskSegment.size() / concurrentThreadNum;
//        log.info("concurrentThreadNum size:{}, each thread execute task size:{}", concurrentThreadNum, batchCount);
//        if (workingTaskSegment.size() % concurrentThreadNum != 0) {
//            batchCount++;
//        }
//        int threadCount = concurrentThreadNum;
//        ExecutorService executorService = executorService();
//        AtomicInteger count = new AtomicInteger();
//        for (int i = 0; i < batchCount; i++) {
//            if (i == batchCount - 1) {
//                threadCount = workingTaskSegment.size() % concurrentThreadNum;
//            }
//            CountDownLatch countDownLatch = new CountDownLatch(threadCount);
//
//            for (int j = 0; j < threadCount; j++) {
//                TaskSegment taskSegment = workingTaskSegment.get(count.getAndIncrement());
//                // 停止具体的子任务
//                if (!isRun()) {
//                    dingErrorLog(MessageFormat.format("快照：{0} 手动停止", taskSegment));
//                    return;
//                }
//                try {
//                    executorService.execute(() -> {
//                        executeTask(taskSegment, doExecuteTask(initialSnapShot));
//                        countDownLatch.countDown();
//                    });
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    dingErrorLog(MessageFormat.format("快照：{0} 执行出现异常：{1}", taskSegment, e));
//                    countDownLatch.countDown();
//                }
//
//            }
//            try {
//                countDownLatch.await();
//            } catch (InterruptedException ex) {
//                ex.printStackTrace();
//            }
//        }
//
//    }

    protected abstract Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot);


    protected void executeTask(TaskSegment taskSegement, Function<TaskSegment, Boolean> function) {
        if (!isRun()) {
            dingErrorLog(MessageFormat.format("快照:{0}已经停止，当前执行快照：{1}", taskSegement.getTaskId(), taskSegement));
            return;
        }
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
        TaskPo.InitialSnapShot initialSnapShot = TaskPo.InitialSnapShot.convertToSnapShot(param);
        initIncrementSync(initialSnapShot);
        return initialSnapShot;
    }

    private static void initIncrementSync(TaskPo.InitialSnapShot initialSnapShot) {
        if (initialSnapShot.isIncrementSync()) {
            LocalDateTime startTime;
            LocalDateTime endTime = LocalDateTime.now();
            // 倒数时间开始
            if (!ObjectUtils.isEmpty(initialSnapShot.getCountDownInterval())) {
                endTime = endTime.plus(TaskUtil.buildTaskDuration(initialSnapShot.getCountDownInterval()));
            } else {
                // 默认从当日0点开始同步
                endTime = LocalDateTime.of(endTime.toLocalDate(), LocalTime.MIN);
            }
            startTime = endTime.plus(TaskUtil.buildDurationPeriod(initialSnapShot.getSyncPeriod()));
            // 小于0 T-n   大于0 T+n
            initialSnapShot.setStartTime(startTime);
            initialSnapShot.setEndTime(endTime);
        }
    }
}
