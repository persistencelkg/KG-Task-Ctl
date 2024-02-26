package org.lkg;

import com.baomidou.mybatisplus.extension.service.IService;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.kg.ctl.core.DataSyncCommonProcessor;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

/**
 * Description: 如何基于脚手架快速开发属于自己数据同步
 * 1. 配置多数据源，因为至少从A到B有两个
 * 2. 引入依赖，更具需要继承
 * <ul>
 *     <li>{@link  org.kg.ctl.core.DataSyncCommonProcessor} : 基于时间段数据同步</li>
 *     <li>{@link  org.kg.ctl.core.DataCheckProcessor} : 数据比对</li>
*      <li>{@link  org.kg.ctl.core.UniqueKeyDataSyncProcessor} : 基于唯一索引数据同步</li>
 *
 * </ul>
 * 3. 配合job 参数，默认基于xxl-job
 * 4. 配置频次控制参数，可基于apollo、nacos配中心等
 * Author: 李开广
 * Date: 2023/10/17 6:58 PM
 */
@Component
public class MyJob extends DataSyncCommonProcessor<Order, Order> {


    public MyJob(DbBatchQueryMapper<Order> from, DbBatchQueryMapper<Order> target, IService<Order> iService) {
        super(from, target, iService);
    }

    @XxlJob("doBatchSyncOrder")
    public void doBatchSyncOrder() {
        super.runTask();
    }

    @Override
    protected List<Order> convertToTargetObject(Collection<Order> sourceData, String tableName) {
        return null;
    }

    @Override
    public boolean idIncrement() {
        return false;
    }

    @Override
    public String uniqueKey() {
        return null;
    }
}
