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

package com.amazon.opendistro.jobscheduler.sampleclient;

import com.amazon.opendistro.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistro.jobscheduler.spi.schedule.Schedule;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;

public class SampleJobParameter implements ScheduledJobParameter {
    // public static final String ID_FIELD = "id";
    public static final String NAME_FIELD = "name";
    public static final String ENABLED_FILED = "enabled";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String SCHEDULE_FIELD = "schedule";
    public static final String ENABLE_TIME_FILED = "enable_time";
    public static final String SAMPLE_PARAM_FILED = "sample_param";

    // private String id;
    private String name;
    private boolean enabled;
    private Instant enableTime;
    private Instant lastUpdateTime;
    private Schedule schedule;
    private String sampleParam;

    public String getSampleParam() {
        return sampleParam;
    }

    public void setSampleParam(String sampleParam) {
        this.sampleParam = sampleParam;
    }

    /*
    @Override
    public String getJobId() {
        return this.id;
    }
    */

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Instant getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    @Override
    public Instant getEnabledTime() {
        return this.enableTime;
    }

    @Override
    public Schedule getSchedue() {
        return this.schedule;
    }

    @Override
    public boolean isEnabled() {
        return this.enabled;
    }

    /*
    public void setId(String id) {
        this.id = id;
    }
    */

    public void setName(String name) {
        this.name = name;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setEnableTime(Instant enableTime) {
        this.enableTime = enableTime;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder//.field(ID_FIELD, this.id)
                .field(NAME_FIELD, this.name)
                .field(ENABLED_FILED, this.enabled)
                .field(SCHEDULE_FIELD, this.schedule)
                .field(SAMPLE_PARAM_FILED, this.sampleParam);
        if(this.enableTime != null) {
            builder.timeField(ENABLE_TIME_FILED, ENABLE_TIME_FILED, this.enableTime.toEpochMilli());
        }
        if(this.lastUpdateTime != null) {
            builder.timeField(LAST_UPDATE_TIME_FIELD, LAST_UPDATE_TIME_FIELD, this.lastUpdateTime.toEpochMilli());
        }
        builder.endObject();
        return builder;
    }
}
