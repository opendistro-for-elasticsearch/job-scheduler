/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.jobscheduler.sampleextension;

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A sample job runner class.
 *
 * The job runner should be a singleton class if it uses Elasticsearch client or other objects passed
 * from Elasticsearch. Because when registering the job runner to JobScheduler plugin, Elasticsearch has
 * not invoke plugins' createComponents() method. That is saying the plugin is not completely initalized,
 * and the Elasticsearch {@link org.elasticsearch.client.Client}, {@link ClusterService} and other objects
 * are not available to plugin and this job runner.
 *
 * So we have to move this job runner intialization to {@link Plugin} createComponents() method, and using
 * singleton job runner to ensure we register a usable job runner instance to JobScheduler plugin.
 *
 * This sample job runner takes the "indexToWatch" from job parameter and logs that index's shards.
 */
public class SampleJobRunner implements ScheduledJobRunner {

    private static final Logger log = LogManager.getLogger(ScheduledJobRunner.class);

    private static SampleJobRunner INSTANCE;

    public static SampleJobRunner getJobRunnerInstance() {
        if(INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (SampleJobRunner.class) {
            if(INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new SampleJobRunner();
            return INSTANCE;
        }
    }

    private ClusterService clusterService;
    private ThreadPool threadPool;

    private SampleJobRunner() {
        // Singleton class, use getJobRunner method instead of constructor
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }
    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        if(!(jobParameter instanceof SampleJobParameter)) {
            throw new IllegalStateException("Job parameter is not instance of SampleJobParameter, type: "
                    + jobParameter.getClass().getCanonicalName());
        }

        if(this.clusterService == null) {
            throw new IllegalStateException("ClusterService is not initialized.");
        }

        if (this.threadPool == null) {
            throw new IllegalStateException("ThreadPool is not initialized.");
        }

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            LockModel lock = null;
            if (jobParameter.getLockDurationSeconds() != null) {
                lock = lockService.acquireLock(
                    SampleExtensionPlugin.JOB_INDEX_NAME,
                    context.getJobId(),
                    jobParameter.getLockDurationSeconds()
                );

                if (lock == null) {
                    return;
                }
            }

            SampleJobParameter parameter = (SampleJobParameter) jobParameter;
            StringBuilder msg = new StringBuilder();
            msg.append("Watching index ").append(parameter.getIndexToWatch()).append("\n");

            List<ShardRouting> shardRoutingList = this.clusterService.state().routingTable().allShards(parameter.getIndexToWatch());
            for(ShardRouting shardRouting : shardRoutingList) {
                msg.append(shardRouting.shardId().getId()).append("\t").append(shardRouting.currentNodeId()).append("\t")
                    .append(shardRouting.active() ? "active" : "inactive").append("\n");
            }
            log.info(msg.toString());

            lockService.release(lock);
        };

        threadPool.generic().submit(runnable);
    }
}
