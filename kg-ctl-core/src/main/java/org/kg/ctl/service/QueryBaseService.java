package org.kg.ctl.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;

/**
 * @description:  基础业务层:统一封装lambad表达式
 * @author: likaiguang
 * @date： 2022/2/10 7:21 下午
 **/
public interface QueryBaseService<T> {
    /**
     * lambada Query
     *
     * @return 通用的lambada查询
     */
    default LambdaQueryWrapper<T> sqlQuery() {
        return Wrappers.lambdaQuery();
    }


    /**
     * lambada Update
     *
     * @return 通用的lambada修改
     */
    default LambdaUpdateWrapper<T> sqlUpdate() {
        return Wrappers.lambdaUpdate();
    }
}
