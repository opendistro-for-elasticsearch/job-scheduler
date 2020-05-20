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

package com.amazon.opendistroforelasticsearch.jobscheduler;

import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobScheduler;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import com.amazon.opendistroforelasticsearch.jobscheduler.sweeper.JobSweeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;


public class JobSchedulerPlugin extends Plugin implements ExtensiblePlugin {

    public static final String OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME = "open_distro_job_scheduler";

    private static final Logger log = LogManager.getLogger(JobSchedulerPlugin.class);

    private JobSweeper sweeper;
    private JobScheduler scheduler;
    private LockService lockService;
    private Map<String, ScheduledJobProvider> indexToJobProviders;
    private Set<String> indicesToListen;

    public JobSchedulerPlugin() {
        this.indicesToListen = new HashSet<>();
        this.indexToJobProviders = new HashMap<>();
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                           ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                           NamedXContentRegistry xContentRegistry, Environment environment,
                           NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                           IndexNameExpressionResolver indexNameExpressionResolver) {
        this.lockService = new LockService(client, clusterService);
        this.scheduler = new JobScheduler(threadPool, this.lockService);
        this.sweeper = initSweeper(environment.settings(), client, clusterService, threadPool, xContentRegistry,
                                   this.scheduler, this.lockService);
        clusterService.addListener(this.sweeper);
        clusterService.addLifecycleListener(this.sweeper);

        return Collections.emptyList();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(JobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingList.add(JobSchedulerSettings.REQUEST_TIMEOUT);
        settingList.add(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingList.add(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingList.add(JobSchedulerSettings.SWEEP_PERIOD);
        settingList.add(JobSchedulerSettings.JITTER_LIMIT);
        return settingList;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int processorCount = EsExecutors.numberOfProcessors(settings);

        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new FixedExecutorBuilder(settings, OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME,
                processorCount, 200, "opendistro.jobscheduler.threadpool"));

        return executorBuilders;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if(this.indicesToListen.contains(indexModule.getIndex().getName())) {
            indexModule.addIndexOperationListener(this.sweeper);
            log.info("JobSweeper started listening to operations on index {}", indexModule.getIndex().getName());
        }
    }

    @Override
    public void reloadSPI(ClassLoader loader) {

        for (JobSchedulerExtension extension : ServiceLoader.load(JobSchedulerExtension.class, loader)) {
            String jobType = extension.getJobType();
            String jobIndexName = extension.getJobIndex();
            ScheduledJobParser jobParser = extension.getJobParser();
            ScheduledJobRunner runner = extension.getJobRunner();
            if(this.indexToJobProviders.containsKey(jobIndexName)) {
                continue;
            }

            ScheduledJobProvider provider = new ScheduledJobProvider(jobType, jobIndexName, jobParser, runner);
            this.indexToJobProviders.put(jobIndexName, provider);
            this.indicesToListen.add(jobIndexName);
            log.info("Loaded scheduler extension: {}, index: {}", jobType, jobIndexName);
        }
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> registryEntries = new ArrayList<>();

        // register schedule
        NamedXContentRegistry.Entry scheduleEntry = new NamedXContentRegistry.Entry(
                Schedule.class,
                new ParseField("schedule"),
                ScheduleParser::parse);
        registryEntries.add(scheduleEntry);

        return registryEntries;
    }

    private JobSweeper initSweeper(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool,
                                   NamedXContentRegistry registry, JobScheduler scheduler, LockService lockService) {
        return new JobSweeper(settings, client, clusterService, threadPool, registry,
                              this.indexToJobProviders, scheduler, lockService);
    }
}
