CREATE TABLE `task`
(
    `id`                int(11) unsigned NOT NULL AUTO_INCREMENT,
    `service_name`      varchar(50)  NOT NULL DEFAULT '' COMMENT '服务名',
    `service_key`       varchar(100) NOT NULL DEFAULT '' COMMENT '服务访问key',
    `task_id`           varchar(32)  NOT NULL DEFAULT '' COMMENT '任务唯一id',
    `mode`              tinyint(1) NOT NULL DEFAULT '0' COMMENT '同步方式',
    `task_dimension`    tinyint(1) NOT NULL DEFAULT '0' COMMENT '任务纬度',
    `task_status`       tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '任务状态',
    `retry_count`       tinyint(1) DEFAULT '0' COMMENT '任务重试次数',
    `create_time`       datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '操作时间',
    `initial_snap_shot` varchar(4096)         DEFAULT '' COMMENT '初始快照',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_service_tid` (`service_name`,`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COMMENT='故障重试-总快照表'


CREATE TABLE `task_segment` (
                                 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                                 `task_id` varchar(32) NOT NULL DEFAULT '' COMMENT '任务唯一id',
                                 `snapshot_value` varchar(1024) DEFAULT '' COMMENT '任务快照json',
                                 `status` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '任务状态',
                                 `start_time` datetime DEFAULT '0000-00-00 00:00:00' COMMENT '任务开始时间',
                                 `end_time` datetime DEFAULT '0000-00-00 00:00:00' COMMENT '任务结束时间',
                                 `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                 `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '操作时间',
                                 `segment_id` int(11) DEFAULT '0' COMMENT '分段id',
                                 `start_index` int(11) DEFAULT '0' COMMENT '起始索引',
                                 `end_index` int(11) DEFAULT '0' COMMENT '结束索引',
                                 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COMMENT='故障重试-子任务表'