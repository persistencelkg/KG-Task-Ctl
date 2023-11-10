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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * TODO: 1.如何服务扩缩容，影响分片任务
 *       2.无xxl-job例如使用了Elastic-Job的分片规则如何确定？
 *         1)单机【无需考虑】 2)集群【可以基于Nacos拉取服务列表来自定义默认的调度策略】
 *       3.分布式调度任务时，不要存储IP，应该以固定InstanceName代替，即只考虑参与任务的机器个数，而不应比较具体的数据， 一旦指定分布式配置，需要给出一个开关进行线程适配。
 * kg task ctl life circle
 * <p>
 * prepareJob->getJob->doGetJob->getProcessingTask-------------
 * initJob -> parseParam -> adaptMode                         |
 * validateJob -> commonCheck -> customCheck                 |
 * createTask -> doCreateTask【splitAndRunTask】             |
 * executeTask;                               <-------------
 * finishJob
 * <p>
 * Author 李开广
 * Date 2023/4/11 7:33 下午
 */
@Slf4j
public abstract class AbstractTaskContext implements TaskMachine, TaskControlService, DingDingService {

    protected static final String UNIFIED = "#";

    @Resource
    @Getter
    private TaskHandler taskHandler;

    @Value("${spring.application.name:''}")
    private String applicationName;

    @Value("${spring.profiles.active:local}")
    private String env;

    private final ThreadLocal<TaskPo> globalTask = ThreadLocal.withInitial(TaskPo::new);
    private final Map<String, Boolean> workingMode = new ConcurrentHashMap<>();

    protected final Map<String, TaskSegment> currentSceneLastSegemntMap = new ConcurrentHashMap<>();


    protected void runTask() {
        try {
            if (isRun()) {
                log.warn("ctl task:{} current has stop，please reopen", getTaskId());
                return;
            }
            TaskPo.InitialSnapShot initialSnapShot = initJob();
            for (; ; ) {
                boolean job = getJob(initialSnapShot.getMode());
                if (!job) {
                    break;
                }
            }
            createTask(initialSnapShot);
        } catch (Exception e) {
            log.error("ctl task error ---->", e);
        }
    }

    private void createTask(TaskPo.InitialSnapShot initialSnapShot) {
        log.info("ctl task:{} start ---->", getTaskId());
        TaskPo taskPo = globalTask.get();
        boolean finish = splitAndRunTask(taskPo, initialSnapShot);
        if (finish) {
            dingErrorLog(MessageFormat.format("ctl task:{0} finished ---->", getTaskId()));
        } else {
            dingErrorLog(MessageFormat.format("ctl task:{0} current cpu time has used up, but not finish job！！！ ---->", getTaskId()));
        }
    }


    private boolean getJob(Integer mode) {
        String simpleName = getTaskId();
        // give other not working mode an opportunity to create task
        String key = simpleName + UNIFIED + mode;
        if (workingMode.containsKey(key)) {
            return workingMode.get(key);
        }
        return doGetJob(simpleName);
    }

    private boolean doGetJob(String simpleName) {
        List<TaskPo> taskJob = taskHandler.listAllModeTask(simpleName);
        if (CollectionUtils.isEmpty(taskJob)) {
            return false;
        }
        if (taskJob.size() > 2) {
            throw new IllegalArgumentException("task:[ " + simpleName + "] exist invalid job count: " + taskJob.size() + "，please check");
        }
        // adapt all_in sync and increment sync run at once
        continueRunTask(simpleName, taskJob);
        return true;
    }

    private void continueRunTask(String taskName, List<TaskPo> taskJob) {
        // increment sync run first
        taskJob.sort(Comparator.comparingInt(TaskPo::getMode));
        boolean isWorking;
        for (TaskPo taskPo : taskJob) {
            isWorking = TaskStatusEnum.WORKING.getCode().equals(taskPo.getTaskStatus());
            TaskPo.InitialSnapShot initialSnapShot = JsonUtil.toBean(taskPo.getInitialSnapShot(), TaskPo.InitialSnapShot.class);
            if (Objects.isNull(initialSnapShot)) {
                throw new IllegalStateException(MessageFormat.format("task id:{0}, initial snapshot damage, please manually repair", taskPo.getTaskId()));
            }
            if (taskPo.isIncrementSync() && Objects.equals(TaskStatusEnum.FINISHED.getCode(), taskPo.getTaskStatus())) {
                initIncrementSync(initialSnapShot);
            }
            workingMode.put(getTaskKey(taskPo), isWorking);
            log.info("task：{} continue execute task, current mode: {}, status:{}", taskName, TaskModeEnum.getInstance(taskPo.getMode()).getDescription(),
                    TaskStatusEnum.getInstance(taskPo.getTaskStatus()).getDescription());
            isWorking = processWorkingSnapshot(taskPo, initialSnapShot);
            workingMode.put(getTaskKey(taskPo), isWorking);
        }
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

    private TaskPo.InitialSnapShot initJob() {
        String param = getParam();
        TaskExecuteParam bean = JsonUtil.toBean(param, TaskExecuteParam.class);
        TaskPo.InitialSnapShot taskSnapShot = instantiationSnapShot(bean);
        // 预校验
        TaskDimensionEnum instance = validateJob(taskSnapShot);
        TaskPo taskJob = new TaskPo();
        taskJob.setTaskId(getTaskId());
        taskJob.setTaskStatus(TaskStatusEnum.WORKING.getCode());
        taskJob.setInitialSnapShot(JsonUtil.toJson(taskSnapShot));
        taskJob.setMode(taskSnapShot.getMode());
        taskJob.setTaskDimension(instance.getDimension());
        // 保存全局快照
        taskHandler.saveOrUpdateSnapshot(taskJob);
        globalTask.set(taskJob);
        log.info("init global snapshot:{} ---> build success, start split segments and run task", taskSnapShot);
        return taskSnapShot;
    }

    @Override
    public String getEnv() {
        return MessageFormat.format("【env:{0}】|【task:{1}】", env, getTaskId());
    }


    protected abstract void checkValid(TaskPo.InitialSnapShot taskSnapShot);


    private TaskDimensionEnum validateJob(TaskPo.InitialSnapShot taskSnapShot) {
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

    //    @Transactional(rollbackFor = Exception.class)
    public boolean splitAndRunTask(TaskPo taskJob, TaskPo.InitialSnapShot taskSnapShot) {
        processTaskByScene(taskJob, taskSnapShot);
        // 直接执行
        return tryFinishJob(taskJob, taskSnapShot);
    }

    protected abstract TaskSegment splitAndRunTask(Integer taskId, TaskPo.InitialSnapShot initialSnapShot, String tableId);


    protected abstract void processTaskByScene(TaskPo taskJob, TaskPo.InitialSnapShot taskSnapShot);

    private boolean processWorkingSnapshot(TaskPo taskJob, TaskPo.InitialSnapShot initialSnapShot) {
        // 获取所有执行中快照
        List<TaskSegment> segmentList = taskHandler.listWorkingSegment(taskJob.getId());
        if (ObjectUtils.isEmpty(segmentList)) {
            TaskSegment taskSegment = taskHandler.listLastSegment(taskJob.getId());
            currentSceneLastSegemntMap.put(getTaskKey(taskJob), taskSegment);
            // 查询最后一个任务，实际切分不是一次性切分，而是预切分
            boolean tryFinishJob = tryFinishJob(taskJob, initialSnapShot);
            if (!tryFinishJob) {
                initialSnapShot.setStartTime(taskSegment.getEndTime());
            }
            return !splitAndRunTask(taskJob, initialSnapShot);
        }
        // 当场快照执行完毕
        dynamicExecuteTask(segmentList, initialSnapShot);
        boolean res  = tryFinishJob(taskJob, initialSnapShot);
        while (!res) {
            initialSnapShot.setStartTime(segmentList.get(segmentList.size() - 1).getEndTime());
            res = !splitAndRunTask(taskJob, initialSnapShot);
        }
        return false;
    }

    protected abstract boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment);

    protected abstract void dynamicExecuteTask(List<TaskSegment> workingTaskSegment, TaskPo.InitialSnapShot initialSnapShot);

    protected abstract Function<TaskSegment, Boolean> buildExecuteFunction(TaskPo.InitialSnapShot initialSnapShot);

    protected void executeTask(TaskSegment taskSegment, Function<TaskSegment, Boolean> function) {
        if (!isRun()) {
            dingErrorLog(MessageFormat.format("快照:{0}已经停止，当前执行快照：{1}", taskSegment.getTaskId(), taskSegment));
            return;
        }
        dingInfoLog(MessageFormat.format("开始执行快照：{0}", taskSegment));
        Boolean apply = function.apply(taskSegment);
        if (!apply) {
            return;
        }
        taskSegment.setStatus(TaskStatusEnum.FINISHED.getCode());
        taskHandler.updateTaskSegment(taskSegment);
        dingInfoLog(MessageFormat.format("快照：{0}执行已完成", taskSegment));
        try {
            TimeUnit.MILLISECONDS.sleep(getSleepTime());
        } catch (InterruptedException ignored) {
        }
    }


    private boolean tryFinishJob(TaskPo taskJob, TaskPo.InitialSnapShot initialSnapShot) {
        TaskSegment taskSegment = currentSceneLastSegemntMap.get(getTaskKey(taskJob));
        if (Objects.isNull(taskSegment)) {
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

    /**
     * 创建初始化快照
     * 具体可以通过 TaskCtlGenerator#getTask实现
     *
     * @return 全局快照
     */
    protected TaskPo.InitialSnapShot instantiationSnapShot(TaskExecuteParam param) {
        TaskPo.InitialSnapShot initialSnapShot = TaskPo.InitialSnapShot.convertToSnapShot(param);
        if (initialSnapShot.isIncrementSync()) {
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
        startTime = endTime.plus(TaskUtil.buildDurationPeriod(initialSnapShot.getSyncPeriod()));
        // 小于0 T-n   大于0 T+n
        initialSnapShot.setStartTime(startTime);
        initialSnapShot.setEndTime(endTime);
    }

    protected String getTaskKey(TaskPo taskJob) {
        return taskJob.getTaskId() + UNIFIED + taskJob.getMode();
    }

}
