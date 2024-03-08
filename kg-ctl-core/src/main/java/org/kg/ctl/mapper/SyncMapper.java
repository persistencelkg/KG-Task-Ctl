package org.kg.ctl.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/3/5 2:50 PM
 */
public interface SyncMapper<T> extends DbBatchQueryMapper<T>, BaseMapper<T> {

    int updateWithClone(@Param("et") T record);

    int insertWithClone(@Param("list") List<T> list);
}
