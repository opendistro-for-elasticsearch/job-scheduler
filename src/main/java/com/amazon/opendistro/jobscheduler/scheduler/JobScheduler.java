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

package com.amazon.opendistro.jobscheduler.scheduler;

import com.amazon.opendistro.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistro.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistro.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistro.jobscheduler.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JobScheduler {
    private static final Logger log = LogManager.getLogger(JobScheduler.class);

    private ThreadPool threadPool;
    private ScheduledJobInfo scheduledJobInfo;
    private Clock clock;

    public JobScheduler(ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.scheduledJobInfo = new ScheduledJobInfo();
        // TODO: need to think more about timezone and instant
        this.clock = Clock.systemDefaultZone();
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

    public boolean schedule(String indexName, String docId, ScheduledJobParameter scheduledJobParameter, ScheduledJobRunner jobRunner) {
        if (!scheduledJobParameter.isEnabled()) {
            return false;
        }
        log.info("Scheduling job id {} for index {} .", docId, indexName);
        JobSchedulingInfo jobInfo;
        synchronized (this.scheduledJobInfo.getJobsByIndex(indexName)) {
            jobInfo = this.scheduledJobInfo.getJobInfo(indexName, docId);
            if (jobInfo == null) {
                jobInfo = new JobSchedulingInfo(docId, scheduledJobParameter);
                this.scheduledJobInfo.addJob(indexName, docId, jobInfo);
            }
            if (jobInfo.getScheduledFuture() != null) {
                return true;
            }

            this.reschedule(scheduledJobParameter, jobInfo, jobRunner);
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
        ScheduledFuture<?> scheduledFuture = jobInfo.getScheduledFuture();

        if (scheduledFuture != null && !scheduledFuture.isDone()) {
            if (FutureUtils.cancel(scheduledFuture)) {
                this.scheduledJobInfo.removeJob(indexName, id);
            } else {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    boolean reschedule(ScheduledJobParameter jobParameter, JobSchedulingInfo jobInfo, ScheduledJobRunner jobRunner) {
        if (jobParameter.getEnabledTime() == null) { // TODO: consider moving enableTime to jobInfo
            log.info("There is no enable time of job {}, this job should never be scheduled.",
                    jobParameter.getName());
            return false;
        }

        Instant nextExecutionTime = jobParameter.getSchedue().getNextExecutionTime(jobInfo.getExpectedExecutionTime());
        if (nextExecutionTime == null) {
            log.info("No next execution time for job {}", jobParameter.getName());
            return true;
        }
        Duration duration = Duration.between(this.clock.instant(), nextExecutionTime);
        jobInfo.setExpectedExecutionTime(nextExecutionTime);

        Runnable runnable = () -> {
            if (jobInfo.isDescheduled()) {
                return;
            }

            jobInfo.setExpectedPreviousExecutionTime(jobInfo.getExpectedExecutionTime());
            jobInfo.setActualPreviousExecutionTime(clock.instant());

            this.reschedule(jobParameter, jobInfo, jobRunner);

            JobExecutionContext context = new JobExecutionContext();
            context.setExpectedExecutionTime(jobInfo.getExpectedPreviousExecutionTime());
            jobRunner.runJob(jobParameter, context);
        };

        // TODO: jobInfo.isDescheduled or jobParameter.isDescheduled ?
        if (jobInfo.isDescheduled()) {
            return false;
        }

        jobInfo.setScheduledFuture(this.threadPool.schedule(new TimeValue(duration.toNanos(), TimeUnit.NANOSECONDS),
                ThreadPool.Names.SAME, runnable));

        return true;
    }

}
