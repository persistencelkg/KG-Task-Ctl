## 分布式任务故障重试脚手架

> 一款基于SpringBoot实现的低代码数据治理脚手架
> 包括不限于：数据同步、数据比对、数据归档、数据恢复能力

## 功能展示
1. 核心代码量【手动coding行数20行以内】
   ![code.png](img%2Fcode.png)
2. 效果展示- 日志收集
   ![log.png](img%2Flog.png)
3. 钉钉同步进度
   ![ding.png](img%2Fding.png)
4. Grafana 可视化监控
   ![perf.png](img%2Fperf.png)
### 引入依赖

```XML
     <dependency>
        <groupId>io.github.persistencelkg</groupId>
        <artifactId>kg-ctl-core</artifactId>
        <version>1.0.2.RELEASE</version>
     </dependency>
```


### 整体框架
![ctl.png](img%2Fctl.png)


### 应用场景
Q1: 你是否开发过N多个框架相似，代码冗长的定时任务内嵌在业务代码中？
A:```kg-ctl```可以减少你80%开发工作量，你仅仅只需要专注于你的同步动作那短短几行的代码逻辑

Q2: 用习惯了黑盒同步工具，如果没有成本部署服务前提下，你如何自己开发一个白盒可控的同步任务？
A: 使用本脚手架将是一个很好的辅助工具

Q3: 想拥有一款断电保护机制，保证你正在执行中的任务优雅的关闭和重启？
A：本脚手架充分考虑这个问题，并提供了该项服务

### 功能

#### v1.0-beta
1. 无缝衔接基于各主流数据库的数据同步操作，包括支持ES等NoSQL
2. 支持多业务维度：按时间批次、按业务id批次、按分表批次等方式进行数据同步 
3. 实现通用数据同步能力、通用数据比对能力
4. 提供灵活的通知能力，目前接入了钉钉通知
5. 适配XXL-JOB、Elastic-JOB等分布式平台

### 效果演示
参考```kg-ctl-quick-start```实践



# 