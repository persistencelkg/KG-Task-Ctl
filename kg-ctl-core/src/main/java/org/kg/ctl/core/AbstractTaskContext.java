package org.kg.ctl.core;


import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskExecuteParam;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskModeEnum;
import org.kg.ctl.service.DingDingService;
import org.kg.ctl.service.TableMetaData;
import org.kg.ctl.service.TaskControlService;
import org.kg.ctl.service.TaskMachine;
import org.kg.ctl.strategy.TaskPostProcessor;
import org.kg.ctl.util.DateTimeUtil;
import org.kg.ctl.util.JsonUtil;
import org.kg.ctl.util.PerfUtil;
import org.kg.ctl.util.TaskUtil;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.kg.ctl.config.JobConstants.QUERY_SIZE_PER_SEC;


/**
 * TODO: 1.如何服务扩缩容，影响分片任务
 *       2.无xxl-job例如使用了Elastic-Job的分片规则如何确定？
 *       3.分布式调度任务时，不要存储IP，应该以固定InstanceName
 * kg task ctl life circle
 * <p>
 * prepareJob
 * initJob -> parseParam -> adaptMode
 * validateJob -> commonCheck -> customCheck
 * createTask -> doCreateTask【splitAndRunTask】
 * executeTask;
 * finishJob
 * <p>
 * Author 李开广
 * Date 2023/4/11 7:33 下午
 */
@Slf4j
public abstract class AbstractTaskContext implements TableMetaData, TaskMachine, TaskControlService, DingDingService {

    protected static final String UNIFIED = "#";

    @Value("${spring.application.name:}")
    private String applicationName;

    @Value("${spring.profiles.active:local}")
    private String env;

    private static final String MODE_KEY = "mode";


    private final ConcurrentHashMap<String, Long> jobGlobalCountMap = new ConcurrentHashMap<>();

    protected void runTask() {
        if (!isRun()) {
            dingInfoLog(MessageFormat.format("ctl task:{0} current has stop，please reopen", getTaskId()));
            return;
        }
        TaskPo.InitialSnapShot currentInitialSnapShot = getCurrentInitialSnapShot();
        MDC.put(MODE_KEY, currentInitialSnapShot.getMode());
        try {
            initJob(currentInitialSnapShot);
            createTask(currentInitialSnapShot);
            if (isRun()) {
                putContext(currentInitialSnapShot, 0L);
            }
        } catch (Exception e) {
            log.error("ctl task error ---->", e);
            dingErrorLog("ctl task exec error");
        } finally {
            MDC.remove(MODE_KEY);
        }
    }

    protected String getMode() {
        return MDC.get(MODE_KEY);
    }

    private void putContext(TaskPo.InitialSnapShot initialSnapShot, long value) {
        jobGlobalCountMap.putIfAbsent(getTaskKey(initialSnapShot.getMode()), value);
    }

    private void createTask(TaskPo.InitialSnapShot initialSnapShot) {
        dingInfoLog(getGlobalInfo(initialSnapShot, "开始执行"));
        splitAndRunTask(initialSnapShot);
        if (isRun()) {
            dingInfoLog(getGlobalInfo(initialSnapShot, "执行完成，恭喜💐💐💐"));
        }
    }

    private void initGlobalCount(TaskPo.InitialSnapShot initialSnapShot) {
        String syncInterval = initialSnapShot.getSyncInterval();
        TemporalAmount temporalAmount = TaskUtil.buildDurationPeriod(syncInterval);
        LocalDateTime startTime = initialSnapShot.getStartTime();
        LocalDateTime endTime = initialSnapShot.getEndTime();

        int count = 0;
        LocalDateTime tempStart = startTime;
        LocalDateTime tempEnd = LocalDateTime.of(startTime.toLocalDate(), LocalTime.MAX);
        long daysBetween = ChronoUnit.DAYS.between(startTime, endTime);
        while (tempStart.isBefore(tempEnd)) {
            count++;
            tempStart = tempStart.plus(temporalAmount);
        }
        putContext(initialSnapShot, count * daysBetween);
    }

    protected int compute(TaskPo.InitialSnapShot initialSnapShot, int i) {
        Long total = jobGlobalCountMap.get(getTaskKey(initialSnapShot.getMode()));
        return (int) (((double) i / total) * 100);
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


    private TaskPo.InitialSnapShot getCurrentInitialSnapShot() {
        String param = getParam();
        Assert.isTrue(!ObjectUtils.isEmpty(param), "not set job param， reject execute");
        TaskExecuteParam bean = JsonUtil.toBean(param, TaskExecuteParam.class);
        return instantiationSnapShot(bean);
    }

    private void initJob(TaskPo.InitialSnapShot taskSnapShot) {
        // 预校验
        initGlobalCount(taskSnapShot);
        log.info("init global snapshot:{}, prepare exec:{} ---> start split segments and run task", taskSnapShot, jobGlobalCountMap);
    }

    @Override
    public String getEnv() {
        return MessageFormat.format("【env:{0}】|【{1}】\n", env, getTaskId());
    }

    public boolean isOnline() {
        return Objects.equals(env, "online") || Objects.equals(env, "prepub");
    }


    protected void checkValid(TaskPo.InitialSnapShot taskSnapShot) {
        if (!taskSnapShot.isProcessUniqueData()) {
            boolean timeValid = Objects.nonNull(taskSnapShot.getStartTime()) && Objects.nonNull(taskSnapShot.getEndTime());
            Assert.isTrue(timeValid, "start_time and end_time is must param");
            Assert.isTrue(taskSnapShot.getStartTime().isBefore(taskSnapShot.getEndTime()), "start_time must less than end_time");
        }
//        if (taskSnapShot.isDivideTable()) {
//            Assert.isTrue(taskSnapShot.isValidDivideTable() > 0, "your choose table id with time range， but not support valid id range");
//        }
        if (idIncrement()) {
            Assert.isTrue(this.getDynamicDbQueryNumber() <= QUERY_SIZE_PER_SEC, "query  size :" + this.getDynamicDbQueryNumber() + " too more， reject request");
        }
    }


    private void validateJob(TaskPo.InitialSnapShot taskSnapShot) {
        // config check
        Assert.isTrue(isNegative(this.getSleepTime()), "[Config Error]：each split task must have valid milliseconds sleep time");
        Assert.isTrue(isNegative(this.getDynamicDbQueryNumber()) && isNegative(this.getTaskSubmitCount()), "[Config Error]：each split task must have valid batch size");
        Assert.isTrue(Objects.nonNull(uniqueKey()), "[Code Error]: each table should have unique key");
        // param check
        taskSnapShot.checkValid();
        // param custom check
        checkValid(taskSnapShot);
    }

    protected String getTaskId() {
        return this.getClass().getSimpleName();
    }

    //    @Transactional(rollbackFor = Exception.class)
    public void splitAndRunTask(TaskPo.InitialSnapShot taskSnapShot) {
        if (taskSnapShot.isDivideTable()) {
            processDivideTableSplitTask(taskSnapShot);
        } else {
            processSimpleTableTask(taskSnapShot, taskSnapShot.getIndex());
        }
    }

    protected abstract void processSimpleTableTask(TaskPo.InitialSnapShot initialSnapShot, String tableId);

    protected abstract void batchExecute(List<TaskSegment> objects, TaskPo.InitialSnapShot initialSnapShot);


    protected void processDivideTableSplitTask(TaskPo.InitialSnapShot taskSnapShot) {

        int tempEnd = 0;
        int end = taskSnapShot.getTableEnd();
        int start = taskSnapShot.getTableStart();
        ExecutorService executorService = executorService();
        while (tempEnd < end) {
            Integer taskSubmitCount = getTaskSubmitCount();
            log.info("{} allocate task submit count:{}", getTaskId(), taskSubmitCount);
            tempEnd = start + taskSubmitCount;
            if (tempEnd > end) {
                tempEnd = end;
            }
            CountDownLatch countDownLatch = new CountDownLatch(tempEnd - start + 1);
            for (int i = start; i <= tempEnd; i++) {
                int finalI = i;
                executorService.execute(() -> {
                    try {
                        if (!isRun()) {
                            return;
                        }
                        String currentTableName = taskSnapShot.getIndex() + finalI;
                        processSimpleTableTask(taskSnapShot, currentTableName);
                        if (!isRun()) {
                            return;
                        }
                        dingInfoLog(MessageFormat.format("{0} has finish task:{1}", currentTableName, taskSnapShot.getTimeRange()));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        dingErrorLog("执行异常" + e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            start = tempEnd + 1;

        }

    }


    protected void doExecuteTask(TaskSegment taskSegment, Function<TaskSegment, Integer> function) {
        if (!isRun()) {
            return;
        }
        TaskModeEnum instance = TaskModeEnum.getInstance(taskSegment.getMode());
        TaskPostProcessor taskPostProcessor = TaskPostProcessor.TASK_MANAGER.get(instance);
        try {
            if (Objects.isNull(taskPostProcessor)) {
                function.apply(taskSegment);
                return;
            }
            taskPostProcessor.postProcessBeforeExecute(taskSegment);
            Integer apply = function.apply(taskSegment);
            if (apply == 0) {
                return;
            }
            taskPostProcessor.postProcessAfterExecute(taskSegment, apply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            dingErrorLog(getPartInfo(taskSegment, "执行异常：" + e, true));
            if (Objects.nonNull(taskSegment.getSnapshotValue())) {
                PerfUtil.countFail(TaskUtil.getPrefixWithOutUnderLine(taskSegment.getSnapshotValue()), instance, 1);
            }
        }
    }

    private boolean tryFinishJob(TaskPo taskJob, TaskSegment lastSegment) {
        // 1. 查询当前任务的按创建时间的最后一个， 根据分表+时间、时间、id进行判断任务是否结束

        if (Objects.isNull(lastSegment)) {
            dingErrorLog(MessageFormat.format("{0}尝试完结任务:{1}失败，请关注", getTaskId(), ""));
            return false;
        }
        TaskPo.InitialSnapShot initialSnapShot = JsonUtil.toBean(taskJob.getInitialSnapShot(), TaskPo.InitialSnapShot.class);

        LocalDateTime endTime = lastSegment.getEndTime();
        boolean timeFinished = endTime.plusNanos(1).isAfter(initialSnapShot.getEndTime());
        boolean divideTable = initialSnapShot.isDivideTable();
        boolean divideTableFinish = false;
        int execEnd = 0;
        if (divideTable) {
            String snapshotValue = lastSegment.getSnapshotValue();
            execEnd = Integer.parseInt(snapshotValue.substring(snapshotValue.lastIndexOf("_") + 1));
            divideTableFinish = execEnd == initialSnapShot.getTableEnd();
        }
        if ((!divideTable && timeFinished) || (divideTable && divideTableFinish && timeFinished)) {
            return true;
        }
        log.info("{}未到任务终点，当前时间进度:{}， 是否分表：{} 分表进度:{}", getTaskId(), DateTimeUtil.format(endTime), divideTable, execEnd);
        return false;
    }

    /**
     * 创建初始化快照
     * 具体可以通过 TaskCtlGenerator#getTask实现
     *
     * @return 全局快照
     */
    protected TaskPo.InitialSnapShot instantiationSnapShot(TaskExecuteParam param) {
        TaskPo.InitialSnapShot initialSnapShot = TaskPo.InitialSnapShot.convertToSnapShot(param);
        validateJob(initialSnapShot);
        if (TaskModeEnum.isIncrementSync(initialSnapShot.getMode())) {
            initIncrementSync(initialSnapShot);
        }
        return initialSnapShot;
    }

    private static void initIncrementSync(TaskPo.InitialSnapShot initialSnapShot) {
        LocalDateTime startTime;
        LocalDateTime endTime = LocalDateTime.now();
        // 倒数时间开始
        if (!ObjectUtils.isEmpty(initialSnapShot.getCountDownInterval())) {
            endTime = endTime.plus(TaskUtil.buildTaskDuration(initialSnapShot.getCountDownInterval()));
        } else {
            // 默认从当日0点开始同步
            endTime = LocalDateTime.of(endTime.toLocalDate(), LocalTime.MIN);
        }
        startTime = endTime.plus(initialSnapShot.convertToPeriod());

        // 小于0 T-n   大于0 T+n
        if (!initialSnapShot.getSyncPeriod().contains("-")) {
            LocalDateTime temp = endTime;
            endTime = startTime;
            startTime = temp;
        }
        initialSnapShot.setStartTime(startTime);
        initialSnapShot.setEndTime(endTime);
    }

    protected String getTaskKey(String mode) {
        return getTaskId() + UNIFIED + mode;
    }
}
