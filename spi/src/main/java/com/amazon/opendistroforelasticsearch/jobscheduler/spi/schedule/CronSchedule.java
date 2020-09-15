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

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.cronutils.utils.VisibleForTesting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

/**
 * UnixCron {@link Schedule} implementation. Refer to https://en.wikipedia.org/wiki/Cron for cron syntax.
 */
public class CronSchedule implements Schedule {
    static final String CRON_FIELD = "cron";
    static final String EXPRESSION_FIELD = "expression";
    static final String TIMEZONE_FIELD = "timezone";

    private static CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));

    private ZoneId timezone;
    private String expression;
    private ExecutionTime executionTime;
    private Clock clock;

    public CronSchedule(String expression, ZoneId timezone) {
        this.expression = expression;
        this.timezone = timezone;
        this.executionTime = ExecutionTime.forCron(cronParser.parse(this.expression));
        clock = Clock.system(timezone);
    }

    public CronSchedule(StreamInput input) throws IOException {
        timezone = input.readZoneId();
        expression = input.readString();
        executionTime = ExecutionTime.forCron(cronParser.parse(expression));
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
                return new Tuple<>(now, now);
            }
            realStartTime = lastExecutionTime.get().toInstant();
        }
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(realStartTime, this.timezone);
        ZonedDateTime newEndTime = executionTime.nextExecution(zonedDateTime).orElse(null);
        return new Tuple<>(realStartTime, newEndTime == null ? null : newEndTime.toInstant());
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

    @Override
    public String toString() {
        return Strings.toString(this, false, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CronSchedule cronSchedule = (CronSchedule) o;
        return timezone.equals(cronSchedule.timezone) &&
                expression.equals(cronSchedule.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timezone, expression);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZoneId(timezone);
        out.writeString(expression);
    }
}
