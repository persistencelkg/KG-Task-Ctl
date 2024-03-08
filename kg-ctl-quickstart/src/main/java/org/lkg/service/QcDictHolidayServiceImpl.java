package org.lkg.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.kg.ctl.service.SyncService;
import org.lkg.mapper.QcHolidayTargetDictMapper;
import org.lkg.pojo.QcHolidayTargetDict;
import org.springframework.stereotype.Service;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/27 4:43 PM
 */
@Service
public class QcDictHolidayServiceImpl extends SyncService<QcHolidayTargetDictMapper, QcHolidayTargetDict> {
}
