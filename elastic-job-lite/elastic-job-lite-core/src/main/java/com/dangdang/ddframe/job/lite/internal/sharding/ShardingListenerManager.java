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

package com.dangdang.ddframe.job.lite.internal.sharding;

import com.dangdang.ddframe.job.lite.internal.config.ConfigurationNode;
import com.dangdang.ddframe.job.lite.internal.config.LiteJobConfigurationGsonFactory;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerNode;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 分片监听管理器.
 *
 * @author zhangliang
 */
public final class ShardingListenerManager extends AbstractListenerManager {

    private final String jobName;

    private final ConfigurationNode configNode;

    private final InstanceNode instanceNode;

    private final ServerNode serverNode;

    private final ShardingService shardingService;

    public ShardingListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
        serverNode = new ServerNode(jobName);
        shardingService = new ShardingService(regCenter, jobName);
    }

    @Override
    public void start() {
        addDataListener(new ShardingTotalCountChangedJobListener());
        addDataListener(new ListenServersChangedJobListener());
    }

    class ShardingTotalCountChangedJobListener extends AbstractJobListener {//场景：对配置节点的数据有更新
        //jobName/config
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (configNode.isConfigPath(path) && 0 != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {//总逻辑是变化path=jobName/config并且任务的总分片数不等于0
                int newShardingTotalCount = LiteJobConfigurationGsonFactory.fromJson(data).getTypeConfig().getCoreConfig().getShardingTotalCount();//配置中心的分片数
                if (newShardingTotalCount != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {//配置中心的分片数和本地的不一致
                    shardingService.setReshardingFlag();//设置标记leader/sharding/necessary 节点内容数据为空
                    JobRegistry.getInstance().setCurrentShardingTotalCount(jobName, newShardingTotalCount);//更新本地的分片数
                }
            }
        }
    }

    class ListenServersChangedJobListener extends AbstractJobListener {//场景：新节点注册，即该job添加了运行实例

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isInstanceChange(eventType, path) || isServerChange(path))) {
                shardingService.setReshardingFlag();
            }
        }
        //jobName/instances 节点有变化，对应有新的节点注册上来
        private boolean isInstanceChange(final Type eventType, final String path) {
            return instanceNode.isInstancePath(path) && Type.NODE_UPDATED != eventType;
        }
        //path=jobName/servers/ip 也是有新的节点注册上来
        private boolean isServerChange(final String path) {
            return serverNode.isServerPath(path);
        }
    }
}
