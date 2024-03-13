package org.lkg.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.kg.ctl.mapper.SyncMapper;
import org.lkg.pojo.QcHolidayTargetDict;

/**
 * Description: 目标数据源同步的mapper
 * Author: 李开广
 * Date: 2024/2/27 4:42 PM
 */
@DS("tidb")
public interface QcHolidayTargetDictMapper extends SyncMapper<QcHolidayTargetDict> {
}
