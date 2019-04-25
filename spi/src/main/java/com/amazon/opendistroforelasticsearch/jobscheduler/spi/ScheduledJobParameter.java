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

package com.amazon.opendistroforelasticsearch.jobscheduler.spi;

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.time.Instant;

/**
 * Job parameters that being used by the JobScheduler.
 */
public interface ScheduledJobParameter extends ToXContentObject {
    /**
     * @return job name.
     */
    String getName();

    /**
     * @return job last update time.
     */
    Instant getLastUpdateTime();

    /**
     * @return get job enabled time.
     */
    Instant getEnabledTime();

    /**
     * @return job schedule.
     */
    Schedule getSchedule();

    /**
     * @return true if job is enabled, false otherwise.
     */
    boolean isEnabled();
}
