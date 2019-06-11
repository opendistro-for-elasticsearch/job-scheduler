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

package com.amazon.opendistroforelasticsearch.jobscheduler.sweeper;

import com.amazon.opendistroforelasticsearch.jobscheduler.JobSchedulerSettings;
import com.amazon.opendistroforelasticsearch.jobscheduler.ScheduledJobProvider;
import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobScheduler;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobDocVersion;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import com.amazon.opendistroforelasticsearch.jobscheduler.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Sweeper component that handles job indexing and cluster changes.
 */
public class JobSweeper extends LifecycleListener implements IndexingOperationListener, ClusterStateListener {
    private static final Logger log = LogManager.getLogger(JobSweeper.class);

    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Map<String, ScheduledJobProvider> indexToProviders;
    private NamedXContentRegistry xContentRegistry;

    private Scheduler.Cancellable scheduledFullSweep;
    private ExecutorService fullSweepExecutor;
    private ConcurrentHashMap<ShardId, ConcurrentHashMap<String, JobDocVersion>> sweptJobs;
    private JobScheduler scheduler;
    private LockService lockService;

    private volatile long lastFullSweepTimeNano;

    private volatile TimeValue sweepPeriod;
    private volatile Integer sweepPageMaxSize;
    private volatile TimeValue sweepSearchTimeout;
    private volatile TimeValue sweepSearchBackoffMillis;
    private volatile Integer sweepSearchBackoffRetryCount;
    private volatile BackoffPolicy sweepSearchBackoff;

    public JobSweeper(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool,
                      NamedXContentRegistry registry, Map<String, ScheduledJobProvider> indexToProviders, JobScheduler scheduler,
                      LockService lockService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.xContentRegistry = registry;
        this.indexToProviders = indexToProviders;
        this.scheduler = scheduler;
        this.lockService = lockService;

        this.lastFullSweepTimeNano = System.nanoTime();
        this.loadSettings(settings);
        this.addConfigListeners();

        this.fullSweepExecutor = Executors.newSingleThreadExecutor(
                EsExecutors.daemonThreadFactory("opendistro_job_sweeper"));
        this.sweptJobs = new ConcurrentHashMap<>();
    }

    private void loadSettings(Settings settings) {
        this.sweepPeriod = JobSchedulerSettings.SWEEP_PERIOD.get(settings);
        this.sweepPageMaxSize = JobSchedulerSettings.SWEEP_PAGE_SIZE.get(settings);
        this.sweepSearchTimeout = JobSchedulerSettings.REQUEST_TIMEOUT.get(settings);
        this.sweepSearchBackoffMillis = JobSchedulerSettings.SWEEP_BACKOFF_MILLIS.get(settings);
        this.sweepSearchBackoffRetryCount = JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT.get(settings);
        this.sweepSearchBackoff = this.updateRetryPolicy();
    }

    private void addConfigListeners() {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_PERIOD,
                timeValue -> {
                    sweepPeriod = timeValue;
                    log.debug("Reinitializing background full sweep with period: " + sweepPeriod.getMinutes());
                    initBackgroundSweep();
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_PAGE_SIZE,
                intValue -> sweepPageMaxSize = intValue);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.REQUEST_TIMEOUT,
                timeValue -> this.sweepSearchTimeout = timeValue);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
                timeValue -> {
                    this.sweepSearchBackoffMillis = timeValue;
                    this.sweepSearchBackoff = this.updateRetryPolicy();
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
                intValue -> {
                    this.sweepSearchBackoffRetryCount = intValue;
                    this.sweepSearchBackoff = this.updateRetryPolicy();
                });
    }

    private BackoffPolicy updateRetryPolicy() {
        return BackoffPolicy.exponentialBackoff(this.sweepSearchBackoffMillis, this.sweepSearchBackoffRetryCount);
    }

    @Override
    public void afterStart() {
        this.initBackgroundSweep();
    }

    @Override
    public void beforeStop() {
        if (this.scheduledFullSweep != null) {
            this.scheduledFullSweep.cancel();
        }
    }

    @Override
    public void beforeClose() {
        this.fullSweepExecutor.shutdown();
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.getResultType().equals(Engine.Result.Type.FAILURE)) {
            log.info("Indexing failed for job {} on index {}", index.id(), shardId.getIndexName());
            return;
        }

        String localNodeId = clusterService.localNode().getId();
        IndexShardRoutingTable routingTable = clusterService.state().routingTable().shardRoutingTable(shardId);
        List<String> shardNodeIds = new ArrayList<>();
        for (ShardRouting shardRouting : routingTable) {
            if (shardRouting.active()) {
                shardNodeIds.add(shardRouting.currentNodeId());
            }
        }

        ShardNodes shardNodes = new ShardNodes(localNodeId, shardNodeIds);
        if (shardNodes.isOwningNode(index.id())) {
            this.sweep(shardId, index.id(), index.source(), new JobDocVersion(result.getTerm(), result.getSeqNo(), result.getVersion()));
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getResultType() == Engine.Result.Type.FAILURE) {
            ConcurrentHashMap<String, JobDocVersion> shardJobs = this.sweptJobs.containsKey(shardId) ?
                    this.sweptJobs.get(shardId) : new ConcurrentHashMap<>();
            JobDocVersion version = shardJobs.get(delete.id());
            log.debug("Deletion failed for scheduled job {}. Continuing with current version {}", delete.id(), version);
            return;
        }

        if (this.scheduler.getScheduledJobIds(shardId.getIndexName()).contains(delete.id())) {
            log.info("Descheduling job {} on index {}", delete.id(), shardId.getIndexName());
            this.scheduler.deschedule(shardId.getIndexName(), delete.id());
            lockService.deleteLock(LockModel.generateLockId(shardId.getIndexName(), delete.id()));
        }
    }

    @VisibleForTesting
    void sweep(ShardId shardId, String docId, BytesReference jobSource, JobDocVersion version) {
        ConcurrentHashMap<String, JobDocVersion> jobVersionMap;
        if (this.sweptJobs.containsKey(shardId)) {
            jobVersionMap = this.sweptJobs.get(shardId);
        } else {
            jobVersionMap = new ConcurrentHashMap<>();
            this.sweptJobs.put(shardId, jobVersionMap);
        }
        jobVersionMap.compute(docId, (id, currentVersion) -> {
            if (version.compareTo(currentVersion) <= 0) {
                log.info("Skipping job {}, new version {} <= current version {}", docId, version, currentVersion);
                return currentVersion;
            }

            if (this.scheduler.getScheduledJobIds(shardId.getIndexName()).contains(docId)) {
                this.scheduler.deschedule(shardId.getIndexName(), docId);
            }
            if (jobSource != null) {
                try {
                    ScheduledJobProvider provider = this.indexToProviders.get(shardId.getIndexName());
                    XContentParser parser = XContentHelper.createParser(this.xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            jobSource, XContentType.JSON);
                    ScheduledJobParameter jobParameter = provider.getJobParser().parse(parser, docId, version.getVersion());
                    if (jobParameter == null) {
                        // allow parser to return null, which means this is not a scheduled job document.
                        return null;
                    }
                    ScheduledJobRunner jobRunner = this.indexToProviders.get(shardId.getIndexName()).getJobRunner();
                    if (jobParameter.isEnabled()) {
                        this.scheduler.schedule(shardId.getIndexName(), docId, jobParameter, jobRunner, version);
                    }
                    return version;
                } catch (Exception e) {
                    log.warn("Unable to parse job, error message: {} , message source: {}", e.getMessage(),
                            Strings.cleanTruncate(jobSource.utf8ToString(), 1000));
                    return currentVersion;
                }
            } else {
                return null;
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        for (String indexName : indexToProviders.keySet()) {
            if (event.indexRoutingTableChanged(indexName)) {
                this.fullSweepExecutor.submit(() -> this.sweepIndex(indexName));
            }
        }
    }

    @VisibleForTesting
    void initBackgroundSweep() {
        if (scheduledFullSweep != null) {
            this.scheduledFullSweep.cancel();
        }

        Runnable scheduledSweep = () -> {
            log.info("Running full sweep");
            TimeValue elapsedTime = getFullSweepElapsedTime();
            long delta = this.sweepPeriod.millis() - elapsedTime.millis();
            if (delta < 20L) {
                this.fullSweepExecutor.submit(this::sweepAllJobIndices);
            }
        };
        this.scheduledFullSweep = this.threadPool.scheduleWithFixedDelay(scheduledSweep, sweepPeriod, ThreadPool.Names.SAME);
    }

    private TimeValue getFullSweepElapsedTime() {
        return TimeValue.timeValueNanos(System.nanoTime() - this.lastFullSweepTimeNano);
    }

    private Map<ShardId, List<ShardRouting>> getLocalShards(ClusterState clusterState, String localNodeId, String indexName) {
        List<ShardRouting> allShards = clusterState.routingTable().allShards(indexName);
        // group shards by shard id
        Map<ShardId, List<ShardRouting>> shards = allShards.stream().filter(ShardRouting::active)
                .collect(Collectors.groupingBy(ShardRouting::shardId,
                        Collectors.mapping(shardRouting -> shardRouting, Collectors.toList())));
        // filter out shards not on local node
        return shards.entrySet().stream().filter((entry) -> entry.getValue().stream()
                .filter((shardRouting -> shardRouting.currentNodeId().equals(localNodeId))).count() > 0)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void sweepAllJobIndices() {
        for (String indexName : this.indexToProviders.keySet()) {
            this.sweepIndex(indexName);
        }
        this.lastFullSweepTimeNano = System.nanoTime();
    }

    private void sweepIndex(String indexName) {
        ClusterState clusterState = this.clusterService.state();
        if (!clusterState.routingTable().hasIndex(indexName)) {
            // deschedule jobs for this index
            for (ShardId shardId : this.sweptJobs.keySet()) {
                if (shardId.getIndexName().equals(indexName) && this.sweptJobs.containsKey(shardId)) {
                    log.info("Descheduling jobs, shard {} index {} as the index is removed.", shardId.getId(), indexName);
                    this.scheduler.bulkDeschedule(shardId.getIndexName(), this.sweptJobs.get(shardId).keySet());
                }
            }
            return;
        }
        String localNodeId = clusterState.getNodes().getLocalNodeId();
        Map<ShardId, List<ShardRouting>> localShards = this.getLocalShards(clusterState, localNodeId, indexName);

        // deschedule jobs in removed shards
        Iterator<Map.Entry<ShardId, ConcurrentHashMap<String, JobDocVersion>>> sweptJobIter = this.sweptJobs.entrySet().iterator();
        while (sweptJobIter.hasNext()) {
            Map.Entry<ShardId, ConcurrentHashMap<String, JobDocVersion>> entry = sweptJobIter.next();
            if (entry.getKey().getIndexName().equals(indexName) && !localShards.containsKey(entry.getKey())) {
                log.info("Descheduling jobs of shard {} index {} as the shard is removed from this node.",
                        entry.getKey().getId(), indexName);
                //shard is removed, deschedule jobs of this shard
                this.scheduler.bulkDeschedule(indexName, entry.getValue().keySet());
                sweptJobIter.remove();
            }
        }

        // sweep each local shard
        for (Map.Entry<ShardId, List<ShardRouting>> shard : localShards.entrySet()) {
            try {
                List<ShardRouting> shardRoutingList = shard.getValue();
                List<String> shardNodeIds = shardRoutingList.stream().map(ShardRouting::currentNodeId).collect(Collectors.toList());
                sweepShard(shard.getKey(), new ShardNodes(localNodeId, shardNodeIds), null);
            } catch (Exception e) {
                log.info("Error while sweeping shard {}, error message: {}", shard.getKey(), e.getMessage());
            }
        }
    }

    private void sweepShard(ShardId shardId, ShardNodes shardNodes, String startAfter) {
        ConcurrentHashMap<String, JobDocVersion> currentJobs = this.sweptJobs.containsKey(shardId) ?
                this.sweptJobs.get(shardId) : new ConcurrentHashMap<>();

        for (String jobId : currentJobs.keySet()) {
            if (!shardNodes.isOwningNode(jobId)) {
                this.scheduler.deschedule(shardId.getIndexName(), jobId);
                currentJobs.remove(jobId);
            }
        }

        String searchAfter = startAfter == null ? "" : startAfter;
        while (searchAfter != null) {
            SearchRequest jobSearchRequest = new SearchRequest()
                    .indices(shardId.getIndexName())
                    .preference("_shards:" + shardId.id() + "|_only_local")
                    .source(new SearchSourceBuilder()
                            .version(true)
                            .seqNoAndPrimaryTerm(true)
                            .sort(new FieldSortBuilder("_id").unmappedType("keyword").missing("_last"))
                            .searchAfter(new String[]{searchAfter})
                            .size(this.sweepPageMaxSize)
                            .query(QueryBuilders.matchAllQuery()));

            SearchResponse response = this.retry((searchRequest) -> this.client.search(searchRequest),
                    jobSearchRequest, this.sweepSearchBackoff).actionGet(this.sweepSearchTimeout);
            if (response.status() != RestStatus.OK) {
                log.error("Error sweeping shard {}, failed querying jobs on this shard", shardId);
                return;
            }
            for (SearchHit hit : response.getHits()) {
                String jobId = hit.getId();
                if (shardNodes.isOwningNode(jobId)) {
                    this.sweep(shardId, jobId, hit.getSourceRef(), new JobDocVersion(hit.getPrimaryTerm(), hit.getSeqNo(),
                            hit.getVersion()));
                }
            }
            if (response.getHits() == null || response.getHits().getHits().length < 1) {
                searchAfter = null;
            } else {
                SearchHit lastHit = response.getHits().getHits()[response.getHits().getHits().length - 1];
                searchAfter = lastHit.getId();
            }
        }
    }

    private <T, R> R retry(Function<T, R> function, T param, BackoffPolicy backoffPolicy) {
        Set<RestStatus> retryalbeStatus = Sets.newHashSet(RestStatus.BAD_GATEWAY, RestStatus.GATEWAY_TIMEOUT,
                RestStatus.SERVICE_UNAVAILABLE);
        Iterator<TimeValue> iter = backoffPolicy.iterator();
        do {
            try {
                return function.apply(param);
            } catch (ElasticsearchException e) {
                if (iter.hasNext() && retryalbeStatus.contains(e.status())) {
                    try {
                        Thread.sleep(iter.next().millis());
                    } catch (InterruptedException ex) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        } while (true);
    }

    private static class ShardNodes {
        private static final int VIRTUAL_NODE_COUNT = 100;

        String localNodeId;
        Collection<String> activeShardNodeIds;
        private TreeMap<Integer, String> circle;

        ShardNodes(String localNodeId, Collection<String> activeShardNodeIds) {
            this.localNodeId = localNodeId;
            this.activeShardNodeIds = activeShardNodeIds;
            this.circle = new TreeMap<>();
            for (String node : activeShardNodeIds) {
                for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                    this.circle.put(Murmur3HashFunction.hash(node + i), node);
                }
            }
        }

        boolean isOwningNode(String jobId) {
            if (this.circle.isEmpty()) {
                return false;
            }
            int jobHashCode = Murmur3HashFunction.hash(jobId);
            String nodeId = this.circle.higherEntry(jobHashCode) == null ? this.circle.firstEntry().getValue()
                    : this.circle.higherEntry(jobHashCode).getValue();
            return this.localNodeId.equals(nodeId);
        }
    }
}
