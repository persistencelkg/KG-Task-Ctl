package org.kg.ctl.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;
import org.kg.ctl.dao.IdRange;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

/**
 * @description: 数据库批量查询基础能力
 * @author: 李开广
 * @date: 2023/5/25 11:12 AM
 */
public interface DbBatchQueryMapper<Source> {

    String SELECT_LIST_WITH_BIZ_ID = "<script>SELECT * FROM ${tableId} WHERE ${targetBizId} in <foreach item='item' collection='ids' open='(' separator=',' close=')'> #{item} </foreach></script>";
    String SELECT_LIST_WITH_TABLE_ID_AND_TIME_RANGE = "SELECT * FROM ${tableId} WHERE id >= #{startId} AND id <= #{maxId} AND ${targetTime} between #{startTime} and #{endTime} LIMIT #{batchSize}";
    String SELECT_LIST_WITH_TIME_RANGE = "SELECT * FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}";

    @Select("SELECT MIN(id) as minId, MAX(id) as maxId  FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}")
    IdRange queryMinIdWithTime(@Param("tableId") String tableId, @Param("targetTime") String targetTime,
                               @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);


    List<Source> selectListWithBizIdList(@Param("tableId")String tableId, @Param("targetBizId")String targetBizId, @Param("ids") Collection<?> collection);


    List<Source> selectListWithTableIdAndTimeRange(@Param("tableId") String tableId, @Param("startId") Long startId, @Param("maxId") Long maxId,
                                                   @Param("batchSize") Integer batchSize, @Param("targetTime") String targetTime,
                                                   @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);

    List<Source> selectListWithTimeRange(@Param("tableId")String tableId,  @Param("targetTime") String targetTime,
                                         @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);
}
