package org.kg.ctl.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/10/17 7:46 PM
 */

@Getter
@AllArgsConstructor
public enum ThreadCountStrategy {
    IO(0){
        @Override
        public int getThreadCount() {
            return INTERNAL_PROCESSORS << 1;
        }
    },
    MIX(1){
        @Override
        public int getThreadCount() {
            return IO.getThreadCount();
        }
    },
    CPU(2) {
        @Override
        public int getThreadCount() {
            return INTERNAL_PROCESSORS;
        }
    }



    ;

    private static final int INTERNAL_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final Map<Integer, ThreadCountStrategy> MAP = new HashMap<Integer, ThreadCountStrategy>();

    static {
        ThreadCountStrategy[] values = values();
        for (ThreadCountStrategy value : values) {
            MAP.put(value.getThreadCount(), value);
        }
    }

    private final Integer bizScene;

   public abstract int getThreadCount();


    public static ThreadCountStrategy getStrategy(String sceneName) {
        try {
            return Enum.valueOf(ThreadCountStrategy.class, sceneName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ThreadCountStrategy.IO;
    }

    public static ThreadCountStrategy getStrategy(Integer scene) {
        ThreadCountStrategy threadCountStrategy = MAP.get(scene);
        return Objects.isNull(threadCountStrategy) ? ThreadCountStrategy.IO : threadCountStrategy;
    }
}
