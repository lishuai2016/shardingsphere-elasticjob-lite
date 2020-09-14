/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.election.LeaderService;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceService;
import com.dangdang.ddframe.job.lite.internal.listener.ListenerManager;
import com.dangdang.ddframe.job.lite.internal.monitor.MonitorService;
import com.dangdang.ddframe.job.lite.internal.reconcile.ReconcileService;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;

import java.util.List;

/**
 * 为调度器提供内部服务的门面类.
 *
 * @author zhangliang
 */
public final class SchedulerFacade {

    private final String jobName;

    private final ConfigurationService configService;//弹性化分布式作业配置服务

    private final LeaderService leaderService;//主节点服务

    private final ServerService serverService;//作业服务器服务

    private final InstanceService instanceService;//作业运行实例服务

    private final ShardingService shardingService;//作业分片服务

    private final ExecutionService executionService;//执行作业的服务

    private final MonitorService monitorService;//作业监控服务

    private final ReconcileService reconcileService;//调解分布式作业不一致状态服务

    private ListenerManager listenerManager;//作业注册中心的监听器管理者

    public SchedulerFacade(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        monitorService = new MonitorService(regCenter, jobName);
        reconcileService = new ReconcileService(regCenter, jobName);
    }

    public SchedulerFacade(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners) {
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        monitorService = new MonitorService(regCenter, jobName);
        reconcileService = new ReconcileService(regCenter, jobName);
        listenerManager = new ListenerManager(regCenter, jobName, elasticJobListeners);//包装zk的监听器管理类
    }

    /**
     * 获取作业触发监听器.
     *
     * @return 作业触发监听器
     */
    public JobTriggerListener newJobTriggerListener() {
        return new JobTriggerListener(executionService, shardingService);
    }

    /**
     * 更新作业配置.
     *
     * @param liteJobConfig 作业配置
     * @return 更新后的作业配置
     */
    public LiteJobConfiguration updateJobConfiguration(final LiteJobConfiguration liteJobConfig) {
        configService.persist(liteJobConfig);
        return configService.load(false);
    }

    /**
     * 注册作业启动信息.
     *
     * @param enabled 作业是否启用
     */
    public void registerStartUpInfo(final boolean enabled) {
        listenerManager.startAllListeners();//注册zk的监听器
        leaderService.electLeader();//选举leader节点ip
        serverService.persistOnline(enabled);//创建 jobname/servers/ip节点
        instanceService.persistOnline();//创建 jobname/instances/ip@-@pid节点
        shardingService.setReshardingFlag();//设置需要进行分片标记leader/sharding/necessary
        monitorService.listen();//
        if (!reconcileService.isRunning()) {
            reconcileService.startAsync();
        }
    }

    /**
     * 终止作业调度.
     */
    public void shutdownInstance() {
        if (leaderService.isLeader()) {
            leaderService.removeLeader();
        }
        monitorService.close();
        if (reconcileService.isRunning()) {
            reconcileService.stopAsync();
        }
        JobRegistry.getInstance().shutdown(jobName);
    }
}
