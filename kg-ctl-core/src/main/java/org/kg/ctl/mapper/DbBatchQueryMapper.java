package org.kg.ctl.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.kg.ctl.dao.IdRange;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

/**
 * Description: 数据库批量查询基础能力
 * Author: 李开广
 * Date: 2023/5/25 11:12 AM
 */
public interface DbBatchQueryMapper<Source> {

    String QUERY_MIN_MAX_ID_WITH_TIME = "SELECT MIN(id) as minId, MAX(id) as maxId  FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}";
    String SELECT_LIST_WITH_BIZ_ID = "<script>SELECT * FROM ${tableId} WHERE ${targetBizId} in <foreach item='item' collection='ids' open='(' separator=',' close=')'> #{item} </foreach></script>";
    String SELECT_LIST_WITH_TABLE_ID_AND_TIME_RANGE = "select * from ${tableId} where id >= #{startId} and id <= #{maxId} and ${targetTime} between #{startTime} and #{endTime} limit #{batchSize}";
    String SELECT_LIST_WITH_TIME_RANGE = "SELECT * FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}";
    String SELECT_COUNT_WITH_TIME_RANGE = "SELECT COUNT(1) FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}";

    @Select(QUERY_MIN_MAX_ID_WITH_TIME)
    IdRange queryMinIdWithTime(@Param("tableId") String tableId, @Param("targetTime") String targetTime,
                               @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);

    @Select(SELECT_LIST_WITH_BIZ_ID)
    List<Source> selectListWithUniqueKeyList(@Param("tableId")String tableId, @Param("targetBizId")String targetBizId, @Param("ids") Collection<?> collection);

    @Select(SELECT_LIST_WITH_TABLE_ID_AND_TIME_RANGE)
    List<Source> selectListWithTableIdAndTimeRange(@Param("tableId") String tableId, @Param("startId") Long startId, @Param("maxId") Long maxId,
                                                   @Param("targetTime") String targetTime,   @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime,
                                                   @Param("batchSize") Integer batchSize);

    @Select(SELECT_LIST_WITH_TIME_RANGE)
    List<Source> selectListWithTimeRange(@Param("tableId")String tableId,  @Param("targetTime") String targetTime,
                                         @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);


    @Select(SELECT_COUNT_WITH_TIME_RANGE)
    Integer selectCountWithTimeRange(@Param("tableId")String tableId,  @Param("targetTime") String targetTime,
                                         @Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);

}
