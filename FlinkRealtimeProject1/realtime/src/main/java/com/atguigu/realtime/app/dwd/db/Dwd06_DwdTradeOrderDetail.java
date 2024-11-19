package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class Dwd06_DwdTradeOrderDetail {

    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 启用状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L))
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 3. 读取 Kafka dwd_trade_order_pre_process 主题数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "order_status string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "operate_date_id string,\n" +
                "operate_time string,\n" +
                "source_id string,\n" +
                "source_type string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "`type` string,\n" +
                "`old` map<string,string>,\n" +
                "od_ts string,\n" +
                "oi_ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + KafkaUtil.getKafkaDDL(
                Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS, "dwd_trade_order_detail"));

        // TODO 4. 过滤下单数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "sku_name,\n" +
                "province_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                "date_id,\n" +
                "create_time,\n" +
                "source_id,\n" +
                "source_type source_type_code,\n" +
                "source_type_name,\n" +
                "sku_num,\n" +
                "split_original_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "split_total_amount,\n" +
                "od_ts ts,\n" +
                "row_op_ts\n" +
                "from dwd_trade_order_pre_process " +
                "where `type`='insert'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        // TODO 5. 创建 Kafka-Connector 下单明细表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + KafkaUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // TODO 6. 将数据写出到 Kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_table");
    }
}

/**
 * FLink on Yarn - Yarn Session 模式运行报错：
 * 2024-11-19 20:21:55
 * org.apache.flink.runtime.JobException: Recovery is suppressed by FailureRateRestartBackoffTimeStrategy(FailureRateRestartBackoffTimeStrategy(failuresIntervalMS=86400000,backoffTimeMS=180000,maxFailuresPerInterval=3)
 * 	at org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:138)
 * 	at org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler.getFailureHandlingResult(ExecutionFailureHandler.java:82)
 * 	at org.apache.flink.runtime.scheduler.DefaultScheduler.handleTaskFailure(DefaultScheduler.java:216)
 * 	at org.apache.flink.runtime.scheduler.DefaultScheduler.maybeHandleTaskFailure(DefaultScheduler.java:206)
 * 	at org.apache.flink.runtime.scheduler.DefaultScheduler.updateTaskExecutionStateInternal(DefaultScheduler.java:197)
 * 	at org.apache.flink.runtime.scheduler.SchedulerBase.updateTaskExecutionState(SchedulerBase.java:682)
 * 	at org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener.notifyTaskFailure(UpdateSchedulerNgOnInternalFailuresListener.java:51)
 * 	at org.apache.flink.runtime.executiongraph.DefaultExecutionGraph.notifySchedulerNgAboutInternalTaskFailure(DefaultExecutionGraph.java:1462)
 * 	at org.apache.flink.runtime.executiongraph.Execution.processFail(Execution.java:1140)
 * 	at org.apache.flink.runtime.executiongraph.Execution.processFail(Execution.java:1080)
 * 	at org.apache.flink.runtime.executiongraph.Execution.fail(Execution.java:783)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot.signalPayloadRelease(SingleLogicalSlot.java:195)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot.release(SingleLogicalSlot.java:182)
 * 	at org.apache.flink.runtime.scheduler.SharedSlot.lambda$release$4(SharedSlot.java:271)
 * 	at java.util.concurrent.CompletableFuture.uniAccept(CompletableFuture.java:656)
 * 	at java.util.concurrent.CompletableFuture.uniAcceptStage(CompletableFuture.java:669)
 * 	at java.util.concurrent.CompletableFuture.thenAccept(CompletableFuture.java:1997)
 * 	at org.apache.flink.runtime.scheduler.SharedSlot.release(SharedSlot.java:271)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.AllocatedSlot.releasePayload(AllocatedSlot.java:152)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool.releasePayload(DefaultDeclarativeSlotPool.java:419)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool.freeAndReleaseSlots(DefaultDeclarativeSlotPool.java:411)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool.releaseSlots(DefaultDeclarativeSlotPool.java:382)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolService.internalReleaseTaskManager(DeclarativeSlotPoolService.java:249)
 * 	at org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolService.releaseTaskManager(DeclarativeSlotPoolService.java:230)
 * 	at org.apache.flink.runtime.jobmaster.JobMaster.disconnectTaskManager(JobMaster.java:497)
 * 	at org.apache.flink.runtime.jobmaster.JobMaster$TaskManagerHeartbeatListener.notifyHeartbeatTimeout(JobMaster.java:1295)
 * 	at org.apache.flink.runtime.heartbeat.HeartbeatMonitorImpl.run(HeartbeatMonitorImpl.java:111)
 * 	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
 * 	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
 * 	at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleRunAsync(AkkaRpcActor.java:440)
 * 	at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleRpcMessage(AkkaRpcActor.java:208)
 * 	at org.apache.flink.runtime.rpc.akka.FencedAkkaRpcActor.handleRpcMessage(FencedAkkaRpcActor.java:77)
 * 	at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleMessage(AkkaRpcActor.java:158)
 * 	at akka.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:26)
 * 	at akka.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:21)
 * 	at scala.PartialFunction$class.applyOrElse(PartialFunction.scala:123)
 * 	at akka.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:21)
 * 	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:170)
 * 	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:171)
 * 	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:171)
 * 	at akka.actor.Actor$class.aroundReceive(Actor.scala:517)
 * 	at akka.actor.AbstractActor.aroundReceive(AbstractActor.scala:225)
 * 	at akka.actor.ActorCell.receiveMessage(ActorCell.scala:592)
 * 	at akka.actor.ActorCell.invoke(ActorCell.scala:561)
 * 	at akka.dispatch.Mailbox.processMailbox(Mailbox.scala:258)
 * 	at akka.dispatch.Mailbox.run(Mailbox.scala:225)
 * 	at akka.dispatch.Mailbox.exec(Mailbox.scala:235)
 * 	at akka.dispatch.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
 * 	at akka.dispatch.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)
 * 	at akka.dispatch.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)
 * 	at akka.dispatch.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)
 * Caused by: java.util.concurrent.TimeoutException: Heartbeat of TaskManager with id container_1732018516864_0001_01_000003(hadoop164:45588) timed out.
 * 	at org.apache.flink.runtime.jobmaster.JobMaster$TaskManagerHeartbeatListener.notifyHeartbeatTimeout(JobMaster.java:1299)
 * 	... 25 more
 */