package org.kg.ctl.service;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.function.Function;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/3/5 2:57 PM
 */
@Service
@Slf4j
public class SyncService<M extends BaseMapper<T>, T> extends ServiceImpl<M, T> {


    public boolean batchOperationWithOutTransaction(Collection<T> collection, Function<T, Integer> consumer) {
        return batchOperationWithOutTransaction(collection, collection.size(), consumer);
    }

    public boolean batchOperationWithOutTransactionLimit1K(Collection<T> collection, Function<T, Integer> consumer) {
        return batchOperationWithOutTransaction(collection, 1_000, consumer);
    }

    public boolean batchOperationWithOutTransaction(Collection<T> collection, int size, Function<T, Integer> consumer) {
        executeBatch(sqlSession -> {
            int i = 1;
            for (T entity : collection) {
                consumer.apply(entity);
                if ((i % size == 0) || i == size) {
                    sqlSession.flushStatements();
                }
                i++;
            }
        });
        return true;
    }
}
