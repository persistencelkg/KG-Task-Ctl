package org.kg.ctl.dao.enums;


import lombok.AllArgsConstructor;
import lombok.Getter;
import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.TaskPo;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.util.HashMap;
import java.util.Objects;

/**
 * @author likaiguang
 * @date 2023/4/13 1:51 下午
 */
@AllArgsConstructor
@Getter
public enum DataSourceEnum {

    /**
     *
     */
    MYSQL("mysql") {
        @Override
        public boolean checkAndInit(String index, TaskPo.InitialSnapShot initialSnapShot) {
            String[] split = index.split(JobConstants.LINE);
            try {
                int start = Integer.parseInt(split[0]);
                int end = Integer.parseInt(split[1]);
                Assert.isTrue(start < end, "未正确配置mysql的分表下标:"  + index);
                initialSnapShot.setTableStart(start);
                initialSnapShot.setTableEnd(end);
                return true;
            } catch (Exception e) {
                Assert.notNull(e, "未正确配置mysql的分表下标:" + index);
                return false;
            }
        }
    },
    ES("es") {
        @Override
        public boolean checkAndInit(String index, TaskPo.InitialSnapShot initialSnapShot) {
            Assert.isTrue(!ObjectUtils.isEmpty(index), "为配置正确的es索引:" + index);
            initialSnapShot.setIndex(index);
            return true;
        }
    };

    private final String ds;

    public abstract boolean checkAndInit(String index, TaskPo.InitialSnapShot initialSnapShot);

    private static HashMap<String, DataSourceEnum> MAP = null;

    private static void init() {
        if (Objects.isNull(MAP)) {
            MAP = new HashMap<>();
        }
        DataSourceEnum[] values = DataSourceEnum.values();
        for (DataSourceEnum value : values) {
            MAP.put(value.getDs(), value);
        }
    }


    public static DataSourceEnum instance(String ds) {
        init();
        return MAP.get(ds);
    }
}
