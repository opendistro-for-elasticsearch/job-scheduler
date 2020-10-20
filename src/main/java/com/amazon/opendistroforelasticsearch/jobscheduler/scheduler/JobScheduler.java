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

package com.amazon.opendistroforelasticsearch.jobscheduler.scheduler;

import com.amazon.opendistroforelasticsearch.jobscheduler.JobSchedulerPlugin;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobDocVersion;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import com.amazon.opendistroforelasticsearch.jobscheduler.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Components that handles job scheduling/descheduling.
 */
public class JobScheduler {
    private static final Logger log = LogManager.getLogger(JobScheduler.class);

    private ThreadPool threadPool;
    private ScheduledJobInfo scheduledJobInfo;
    private Clock clock;
    private final LockService lockService;

    public JobScheduler(ThreadPool threadPool, final LockService lockService) {
        this.threadPool = threadPool;
        this.scheduledJobInfo = new ScheduledJobInfo();
        this.clock = Clock.systemDefaultZone();
        this.lockService = lockService;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    @VisibleForTesting
    ScheduledJobInfo getScheduledJobInfo() {
        return this.scheduledJobInfo;
    }

    public Set<String> getScheduledJobIds(String indexName) {
        return this.scheduledJobInfo.getJobsByIndex(indexName).keySet();
    }

    public boolean schedule(String indexName, String docId, ScheduledJobParameter scheduledJobParameter,
                            ScheduledJobRunner jobRunner, JobDocVersion version, Double jitterLimit) {
        if (!scheduledJobParameter.isEnabled()) {
            return false;
        }
        log.info("Scheduling job id {} for index {} .", docId, indexName);
        JobSchedulingInfo jobInfo;
        synchronized (this.scheduledJobInfo.getJobsByIndex(indexName)) {
            jobInfo = this.scheduledJobInfo.getJobInfo(indexName, docId);
            if (jobInfo == null) {
                jobInfo = new JobSchedulingInfo(indexName, docId, scheduledJobParameter);
                this.scheduledJobInfo.addJob(indexName, docId, jobInfo);
            }
            if (jobInfo.getScheduledCancellable() != null) {
                return true;
            }

            this.reschedule(scheduledJobParameter, jobInfo, jobRunner, version, jitterLimit);
        }

        return true;
    }

    public List<String> bulkDeschedule(String indexName, Collection<String> ids) {
        if (ids == null) {
            return new ArrayList<>();
        }
        List<String> result = new ArrayList<>();
        for (String id : ids) {
            if (!this.deschedule(indexName, id)) {
                result.add(id);
                log.error("Unable to deschedule job {}", id);
            }
        }
        return result;
    }

    public boolean deschedule(String indexName, String id) {
        JobSchedulingInfo jobInfo = this.scheduledJobInfo.getJobInfo(indexName, id);
        if (jobInfo == null) {
            log.debug("JobId {} doesn't not exist, skip descheduling.", id);
            return true;
        }

        log.info("Descheduling jobId: {}", id);
        jobInfo.setDescheduled(true);
        jobInfo.setActualPreviousExecutionTime(null);
        jobInfo.setExpectedPreviousExecutionTime(null);
        Scheduler.ScheduledCancellable scheduledCancellable = jobInfo.getScheduledCancellable();

        if (scheduledCancellable != null) {
            if (scheduledCancellable.cancel()) {
                this.scheduledJobInfo.removeJob(indexName, id);
            } else {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    boolean reschedule(ScheduledJobParameter jobParameter, JobSchedulingInfo jobInfo, ScheduledJobRunner jobRunner,
                       JobDocVersion version, Double jitterLimit) {
        if (jobParameter.getEnabledTime() == null) {
            log.info("There is no enable time of job {}, this job should never be scheduled.",
                    jobParameter.getName());
            return false;
        }

        Instant nextExecutionTime = jobParameter.getSchedule().getNextExecutionTime(jobInfo.getExpectedExecutionTime());
        if (nextExecutionTime == null) {
            log.info("No next execution time for job {}", jobParameter.getName());
            return true;
        }
        Duration duration = Duration.between(this.clock.instant(), nextExecutionTime);

        // Too many jobs start at the same time point will bring burst. Add random jitter delay to spread out load.
        // Example, if interval is 10 minutes, jitter is 0.6, next job run will be randomly delayed by 0 to 10*0.6 minutes.
        Instant secondExecutionTimeFromNow = jobParameter.getSchedule().getNextExecutionTime(nextExecutionTime);
        if (secondExecutionTimeFromNow != null) {
            Duration interval = Duration.between(nextExecutionTime, secondExecutionTimeFromNow);
            if (interval.toMillis() > 0) {
                double jitter = jobParameter.getJitter() == null ? 0d : jobParameter.getJitter();
                jitter = jitter > jitterLimit ? jitterLimit : jitter;
                jitter = jitter < 0 ? 0 : jitter;
                long jitterMillis = Math.round(Randomness.get().nextLong() % interval.toMillis() * jitter);
                if (jitter > 0) {
                    log.info("Will delay {} miliseconds for next execution of job {}", jitterMillis, jobParameter.getName());
                }
                duration = duration.plusMillis(jitterMillis);
            }
        }

        jobInfo.setExpectedExecutionTime(nextExecutionTime);

        Runnable runnable = () -> {
            if (jobInfo.isDescheduled()) {
                return;
            }

            jobInfo.setExpectedPreviousExecutionTime(jobInfo.getExpectedExecutionTime());
            jobInfo.setActualPreviousExecutionTime(clock.instant());
            // schedule next execution
            this.reschedule(jobParameter, jobInfo, jobRunner, version, jitterLimit);

            // invoke job runner
            JobExecutionContext context = new JobExecutionContext(jobInfo.getExpectedPreviousExecutionTime(), version, lockService,
                jobInfo.getIndexName(), jobInfo.getJobId());

            jobRunner.runJob(jobParameter, context);
        };

        if (jobInfo.isDescheduled()) {
            return false;
        }

        jobInfo.setScheduledCancellable(this.threadPool.schedule(runnable, new TimeValue(duration.toNanos(),
                        TimeUnit.NANOSECONDS), JobSchedulerPlugin.OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME));

        return true;
    }

    public List<JobSchedulerMetrics> getJobSchedulerMetrics(String indexName) {
        return this.scheduledJobInfo.getJobsByIndex(indexName).entrySet().stream()
                .map(entry -> {
                    Instant lastExecutionTime = entry.getValue().getActualPreviousExecutionTime();
                    Instant nextExecutionTime = entry.getValue().getExpectedExecutionTime();
                    return new JobSchedulerMetrics(
                            entry.getValue().getJobId(),
                            lastExecutionTime == null ? null : lastExecutionTime.toEpochMilli(),
                            nextExecutionTime == null ? null : nextExecutionTime.toEpochMilli(),
                            entry.getValue().getScheduledCancellable().getDelay(TimeUnit.MILLISECONDS));
                })
                .collect(Collectors.toList());
    }
}
