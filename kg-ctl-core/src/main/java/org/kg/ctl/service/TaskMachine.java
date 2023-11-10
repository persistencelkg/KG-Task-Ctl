package org.kg.ctl.service;

import com.xxl.job.core.context.XxlJobHelper;

/**
 * Author 李开广
 */
public interface TaskMachine {

   default String getParam() {
       return XxlJobHelper.getJobParam();
   }


   default int getIndex() {
       int jobId = XxlJobHelper.getShardIndex();
       return Math.max(jobId, 0);
   }

    default int getTotalCount() {
        int count = XxlJobHelper.getShardTotal();
        return Math.max(count, 0);
    }
}
