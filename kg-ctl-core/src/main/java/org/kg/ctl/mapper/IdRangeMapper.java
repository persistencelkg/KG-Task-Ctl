package org.kg.ctl.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.apache.ibatis.annotations.*;
import org.kg.ctl.dao.IdRange;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/5/25 4:14 PM
 */
@Repository
@Mapper
@DS("${source.ds}")
public interface IdRangeMapper {

    @Select("SELECT MIN(id) as minId, MAX(id) as maxId  FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}")
    @ResultType(IdRange.class)
    IdRange queryMinIdWithTime(@Param("tableId") String tableId, @Param("targetTime") String targetTime,
                               @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);


}
