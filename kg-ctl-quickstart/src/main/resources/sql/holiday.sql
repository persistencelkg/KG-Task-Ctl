CREATE TABLE `qc_holiday_dict` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`each_day` date NOT NULL COMMENT '每一天',
`each_day_name` varchar(10) NOT NULL DEFAULT '' COMMENT '每天的名称：ag: 周一、周日',
`each_year` int(4) NOT NULL COMMENT '年份',
`is_off_day` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否是休息日[非工作日]，1是',
`is_official_holiday` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否是法定节假日，1是',
`is_week` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否是周六日, 1是',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '操作时间',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_each_day` (`each_day`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COMMENT='节假日字典'



