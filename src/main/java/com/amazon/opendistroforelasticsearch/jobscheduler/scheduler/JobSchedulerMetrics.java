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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;

public final class JobSchedulerMetrics implements ToXContentFragment, Writeable {
    private String scheduledJobId;
    private Long lastExecutionTime;
    private Long nextExecutionTime;
    private Long durationToNextExecution;

    public JobSchedulerMetrics(String scheduledJobId, Long lastExecutionTime,
                               Long expectedNextExecutionTime, Long durationToNextExecution) {
        this.scheduledJobId = scheduledJobId;
        this.lastExecutionTime = lastExecutionTime;
        this.nextExecutionTime = expectedNextExecutionTime;
        this.durationToNextExecution = durationToNextExecution;
    }

    public JobSchedulerMetrics(StreamInput in) throws IOException {
        scheduledJobId = in.readString();
        lastExecutionTime = in.readOptionalLong();
        nextExecutionTime = in.readOptionalLong();
        durationToNextExecution = in.readOptionalLong();
    }

    public String getScheduledJobId() {
        return scheduledJobId;
    }

    public Long getLastExecutionTime() {
        return lastExecutionTime;
    }

    public Long getNextExecutionTime() {
        return nextExecutionTime;
    }

    public Long getDurationToNextExecution() {
        return durationToNextExecution;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.scheduledJobId);
        out.writeOptionalLong(this.lastExecutionTime);
        out.writeOptionalLong(this.nextExecutionTime);
        out.writeOptionalLong(this.durationToNextExecution);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (lastExecutionTime != null) {
            builder.timeField("last_execution_time", "last_execution_time", Instant.ofEpochMilli(this.lastExecutionTime).toEpochMilli());
        }
        if (nextExecutionTime != null) {
            builder.timeField("next_execution_time", "next_execution_time", Instant.ofEpochMilli(this.nextExecutionTime).toEpochMilli());
        }
        if (durationToNextExecution != null) {
            builder.field("duration_to_next_execution_in_ms", this.durationToNextExecution);
        }
        return builder;
    }
}
