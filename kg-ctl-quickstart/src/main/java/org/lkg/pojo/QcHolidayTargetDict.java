package org.lkg.pojo;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.util.Date;

/**
 * Description: 目的数据表
 * Author: 李开广
 * Date: 2024/2/27 3:50 PM
 */
@Data
public class QcHolidayTargetDict {

    @TableId(type = IdType.INPUT)
    // 默认一般不是id，
    private Integer id;
    private Date eachDay;
    private String eachDayName;

    private Integer eachYear;

    private Integer isOffDay;

    private Integer isOfficialHoliday;

    private Integer isWeek;

    private Date createTime;
    private Date updateTime;
}
