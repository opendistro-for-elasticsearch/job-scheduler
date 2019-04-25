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

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import org.elasticsearch.threadpool.Scheduler;

import java.time.Instant;

class JobSchedulingInfo {
    private String jobId;
    private ScheduledJobParameter jobParameter;
    private boolean descheduled = false;
    private Instant actualPreviousExecutionTime;
    private Instant expectedPreviousExecutionTime;
    private Instant expectedExecutionTime;
    private Scheduler.ScheduledCancellable scheduledCancellable;

    JobSchedulingInfo(String jobId, ScheduledJobParameter jobParameter) {
        this.jobId = jobId;
        this.jobParameter = jobParameter;
    }

    public String getJobId() {
        return jobId;
    }

    public ScheduledJobParameter getJobParameter() {
        return jobParameter;
    }

    public boolean isDescheduled() {
        return descheduled;
    }

    public Instant getActualPreviousExecutionTime() {
        return actualPreviousExecutionTime;
    }

    public Instant getExpectedPreviousExecutionTime() {
        return expectedPreviousExecutionTime;
    }

    public Instant getExpectedExecutionTime() {
        return this.expectedExecutionTime;
    }

    public Scheduler.ScheduledCancellable getScheduledCancellable() {
        return scheduledCancellable;
    }

    public void setDescheduled(boolean descheduled) {
        this.descheduled = descheduled;
    }

    public void setActualPreviousExecutionTime(Instant actualPreviousExecutionTime) {
        this.actualPreviousExecutionTime = actualPreviousExecutionTime;
    }

    public void setExpectedPreviousExecutionTime(Instant expectedPreviousExecutionTime) {
        this.expectedPreviousExecutionTime = expectedPreviousExecutionTime;
    }

    public void setExpectedExecutionTime(Instant expectedExecutionTime) {
        this.expectedExecutionTime = expectedExecutionTime;
    }

    public void setScheduledCancellable(Scheduler.ScheduledCancellable scheduledCancellable) {
        this.scheduledCancellable = scheduledCancellable;
    }

}
