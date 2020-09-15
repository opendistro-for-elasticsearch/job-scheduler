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

package com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.time.Duration;
import java.time.Instant;

public interface Schedule extends Writeable, ToXContentObject {

    /**
     * Gets next job execution time of give time parameter.
     *
     * @param time base time point
     * @return next exection time since time parameter.
     */
    Instant getNextExecutionTime(Instant time);

    /**
     * Calculates the time duration between next execution time and now.
     *
     * @return time duration between next execution and now.
     */
    Duration nextTimeToExecute();

    /**
     * Gets the execution period starting at {@code startTime}.
     *
     * @param startTime start time of the period.
     * @return the start time and end time of the period in the tuple.
     */
    Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime);

    /**
     * Returns if the job is running on time.
     *
     * @param lastExecutionTime last execution time.
     * @return true if the job executes on time, otherwise false.
     */
    Boolean runningOnTime(Instant lastExecutionTime);
}