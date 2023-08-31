package org.kg.ctl.config;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.kg.ctl.util.DateTimeUtil;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * sharding-jdbc没有实现这个LocalDateTime的转换，这里自定义转换类
 * @author: qsy
 * @date: 2023/2/16 16:10
 */
public class LocalDateTimeTypeHandler extends BaseTypeHandler<LocalDateTime> {
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, LocalDateTime parameter, JdbcType jdbcType) throws SQLException {
        ps.setObject(i,parameter);
    }

    @Override
    public LocalDateTime getNullableResult(ResultSet rs, String columnName) throws SQLException {
        return DateTimeUtil.parse(rs.getObject(columnName).toString());
    }

    @Override
    public LocalDateTime getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        return  DateTimeUtil.parse(rs.getObject(columnIndex).toString());
    }

    @Override
    public LocalDateTime getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        return DateTimeUtil.parse(cs.getObject(columnIndex).toString());
    }
}
