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

    /**
     * @return Null if scheduled job doesn't need lock. Seconds of lock duration if the scheduled job needs to be a singleton runner.
     */
    default Long getLockDurationSeconds() {
        return null;
    }

    /**
     * Job will be delayed randomly with range of (0, jitter)*interval for the
     * next execution time. For example, if next run is 10 minutes later, jitter
     * is 0.6, then next job run will be randomly delayed by 0 to 6 minutes.
     *
     * Jitter is percentage, so it should be positive and less than 1.
     * <p>
     * <b>Note:</b> default logic for these cases:
     * 1).If jitter is not set, will regard it as 0.0.
     * 2).If jitter is negative, will reset it as 0.0.
     * 3).If jitter exceeds jitter limit, will cap it as jitter limit. Default
     * jitter limit is 0.95. So if you set jitter as 0.96, will cap it as 0.95.
     *
     * @return job execution jitter
     */
    default Double getJitter() {return null;}

    /**
     * The job will be delayed for a fixed amount of time for every execution.
     * It will be reset to 0 if a negative number is given.
     *
     * The delay will be evaluated before every execution of the job,
     * if it exceeds the time interval between the first and the second execution after the evaluation time,
     * it will be cut down to be the same with the time interval.
     *
     * @return the fixed delay in seconds before the scheduled job executes. Null if there is no fixed delay.
     */
    default Long getDelaySeconds() {
        return null;
    }
}
