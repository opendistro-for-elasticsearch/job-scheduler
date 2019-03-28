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

package com.amazon.opendistro.jobscheduler.spi.schedule;

import com.cronutils.model.time.ExecutionTime;
import com.cronutils.utils.VisibleForTesting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class CronSchedule implements Schedule {

    static final String CRON_FIELD = "cron";
    static final String EXPRESSION_FIELD = "expression";
    static final String TIMEZONE_FIELD = "timezone";

    private ZoneId timezone;
    private String expression;
    private ExecutionTime executionTime;
    private Clock clock;

    public CronSchedule(String expression, ZoneId timezone) {
        this.expression = expression;
        this.timezone = timezone;
        this.executionTime = ExecutionTime.forCron(Schedule.cronParser.parse(this.expression));
        clock = Clock.system(timezone);
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    @VisibleForTesting
    void setExecutionTime(ExecutionTime executionTime) {
        this.executionTime = executionTime;
    }

    @VisibleForTesting
    ZoneId getTimeZone() {
        return this.timezone;
    }

    @VisibleForTesting
    String getCronExpression() {
        return this.expression;
    }

    @Override
    public Instant getNextExecutionTime(Instant time) {
        Instant baseTime = time == null ? this.clock.instant() : time;

        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(baseTime, this.timezone);
        ZonedDateTime nextExecutionTime = this.executionTime.nextExecution(zonedDateTime).orElse(null);

        return nextExecutionTime == null ? null : nextExecutionTime.toInstant();
    }

    @Override
    public Duration nextTimeToExecute() {
        Instant now = this.clock.instant();
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(now, this.timezone);
        Optional<Duration> timeToNextExecution = this.executionTime.timeToNextExecution(zonedDateTime);
        return timeToNextExecution.orElse(null);
    }

    @Override
    public Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime) {
        Instant realStartTime;
        if (startTime != null) {
            realStartTime = startTime;
        } else {
            Instant now = this.clock.instant();
            Optional<ZonedDateTime> lastExecutionTime = this.executionTime.lastExecution(ZonedDateTime.ofInstant(now, this.timezone));
            if (!lastExecutionTime.isPresent()) {
                Instant currentTime = now;
                return new Tuple<>(currentTime, currentTime);
            }
            realStartTime = lastExecutionTime.get().toInstant();
        }
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(realStartTime, this.timezone);
        ZonedDateTime newEndTime = executionTime.nextExecution(zonedDateTime).orElse(null);
        return new Tuple<Instant, Instant>(realStartTime, newEndTime == null ? null : newEndTime.toInstant());
    }

    @Override
    public Boolean runningOnTime(Instant lastExecutionTime) {
        if (lastExecutionTime == null) {
            return true;
        }

        Instant now = this.clock.instant();
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(now, timezone);
        Optional<ZonedDateTime> expectedExecutionTime = this.executionTime.lastExecution(zonedDateTime);

        if (!expectedExecutionTime.isPresent()) {
            return false;
        }
        ZonedDateTime actualExecutionTime = ZonedDateTime.ofInstant(lastExecutionTime, timezone);

        return ChronoUnit.SECONDS.between(expectedExecutionTime.get(), actualExecutionTime) == 0L;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .startObject(CRON_FIELD)
                .field(EXPRESSION_FIELD, this.expression)
                .field(TIMEZONE_FIELD, this.timezone.getId())
                .endObject()
                .endObject();
        return builder;
    }
}
