package org.kg.ctl.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.kg.ctl.dao.IdRange;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/5/25 4:14 PM
 */
@Repository
public interface IdRangeMapper {

    @Select("SELECT MIN(id) as minId, MAX(id) as maxId  FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}")
    IdRange queryMinIdWithTime(@Param("tableId") String tableId, @Param("targetTime") String targetTime,
                               @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);


}
