package org.lkg;

import com.xxl.job.core.handler.annotation.XxlJob;
import org.aspectj.weaver.ast.Or;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.strategy.impl.TableIndexRangeProcessor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Description: 如何基于脚手架快速开发属于自己数据同步
 * 1. 配置多数据源，因为至少从A到B有两个
 * 2. 引入依赖，从三个粒度【按业务id同步、按时间同步、按分表同步】选取
 * 3. 配置任务频次，见配置文件
 * 4. 拉取认为
 * Author: 李开广
 * Date: 2023/10/17 6:58 PM
 */
@Component
public class MyJob extends TableIndexRangeProcessor<Order> {


    public MyJob(DbBatchQueryMapper<Order> dbBatchQueryMapper) {
        super(dbBatchQueryMapper);
    }

    @XxlJob("doBatchSyncOrder")
    public void doBatchSyncOrder() {
        super.runTask();
    }


    @Override
    protected void batchToTarget(Collection<Order> sourceData) {
        // 写入你的目标数据库逻辑，比如
        List<?> list = new ArrayList<>();
        for (Order sourceDatum : sourceData) {
            // 完成你的目的数据类型的转换
            // list.add(convert(sourceDatum))
        }
        // syncToTidb();
        // syncToES();
        // 注意你无需开发多线程，因为改操作已经内置，sourceData的数据取决你的配置大小，你只需要考虑怎么同步到你的目标服务上，
        // 例如tidbService.bathInsert(sourceData);
    }
}
