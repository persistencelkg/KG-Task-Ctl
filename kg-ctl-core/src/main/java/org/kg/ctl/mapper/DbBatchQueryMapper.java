package org.kg.ctl.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

/**
 * @description: 数据库批量查询基础能力
 * @author: 李开广
 * @date: 2023/5/25 11:12 AM
 */
@Repository
public interface DbBatchQueryMapper<Source> {

    @Select("<script>SELECT * FROM ${tableId} WHERE ${targetBizId} in " +
            "<foreach item='item' collection='ids' open='(' separator=',' close=')'> #{item} </foreach></script>")
    List<Source> selectListWithBizIdList(String tableId, String targetBizId, @Param("ids") Collection<?> collection);

    @Select("SELECT * FROM #{tableId} WHERE id >= #{startId} AND id <= #{maxId}" +
            "AND ${targetTime} between #{startTime} and #{endTime} LIMIT #{batchSize}")
    List<Source> selectList(String tableId, Long startId, Long maxId, Integer batchSize, String targetTime, LocalDateTime startTime, LocalDateTime endTime);


    @Select("SELECT * FROM ${tableId} WHERE ${targetTime} between #{startTime} and #{endTime}")
    List<Source> selectListForSync(String tableId, String targetTime, LocalDateTime startTime, LocalDateTime endTime);
}
