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

import com.cronutils.utils.VisibleForTesting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link Schedule} defined by interval (interval value & interval unit). Currently the finest unit supported is minute.
 */
public class IntervalSchedule implements Schedule {

    static final String START_TIME_FIELD = "start_time";
    static final String INTERVAL_FIELD = "interval";
    static final String PERIOD_FIELD = "period";
    static final String UNIT_FIELD = "unit";

    private static final Set<ChronoUnit> SUPPORTED_UNITS;

    static {
        HashSet<ChronoUnit> set = new HashSet<>();
        set.add(ChronoUnit.MINUTES);
        set.add(ChronoUnit.HOURS);
        set.add(ChronoUnit.DAYS);
        SUPPORTED_UNITS = Collections.unmodifiableSet(set);
    }

    private Instant startTime;
    private int interval;
    private ChronoUnit unit;
    private transient long intervalInMillis;
    private Clock clock;

    public IntervalSchedule(Instant startTime, int interval, ChronoUnit unit) {
        if (!SUPPORTED_UNITS.contains(unit)) {
            throw new IllegalArgumentException(String.format("Interval unit %s is not supported, expects %s", unit, SUPPORTED_UNITS));
        }
        this.startTime = startTime;
        this.interval = interval;
        this.unit = unit;
        this.intervalInMillis = Duration.of(interval, this.unit).toMillis();
        this.clock = Clock.system(ZoneId.systemDefault());
    }

    @VisibleForTesting
    Instant getStartTime() {
        return this.startTime;
    }

    @VisibleForTesting
    public int getInterval() {
        return this.interval;
    }

    public ChronoUnit getUnit() {
        return this.unit;
    }

    @Override
    public Instant getNextExecutionTime(Instant time) {
        Instant baseTime = time == null ? this.clock.instant() : time;
        long delta = (baseTime.toEpochMilli() - this.startTime.toEpochMilli()) % this.intervalInMillis;
        long remaining = this.intervalInMillis - delta;

        return baseTime.plus(remaining, ChronoUnit.MILLIS);
    }

    @Override
    public Duration nextTimeToExecute() {
        long enabledTimeEpochMillis = this.startTime.toEpochMilli();
        Instant currentTime = this.clock.instant();
        long delta = currentTime.toEpochMilli() - enabledTimeEpochMillis;
        long remainingScheduleTime = intervalInMillis - (delta % intervalInMillis);
        return Duration.of(remainingScheduleTime, ChronoUnit.MILLIS);
    }

    @Override
    public Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime) {
        Instant realStartTime = startTime == null ? this.clock.instant() : startTime;
        Instant newEndTime = realStartTime.plusMillis(this.intervalInMillis);
        return new Tuple<>(realStartTime, newEndTime);
    }

    @Override
    public Boolean runningOnTime(Instant lastExecutionTime) {
        if (lastExecutionTime == null) {
            return true;
        }

        long enabledTimeEpochMillis = this.startTime.toEpochMilli();
        Instant now = this.clock.instant();
        long expectedMillisSinceLastExecution = (now.toEpochMilli() - enabledTimeEpochMillis) % this.intervalInMillis;
        if (expectedMillisSinceLastExecution < 1000) {
            expectedMillisSinceLastExecution = this.intervalInMillis + expectedMillisSinceLastExecution;
        }
        long expectedLastExecutionTime = now.toEpochMilli() - expectedMillisSinceLastExecution;
        long expectedCurrentExecutionTime = expectedLastExecutionTime + this.intervalInMillis;
        return Math.abs(lastExecutionTime.toEpochMilli() - expectedLastExecutionTime) < 1000
                || Math.abs(lastExecutionTime.toEpochMilli() - expectedCurrentExecutionTime) < 1000;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .startObject(INTERVAL_FIELD)
                .field(START_TIME_FIELD, this.startTime.toEpochMilli())
                .field(PERIOD_FIELD, this.interval)
                .field(UNIT_FIELD, this.unit)
                .endObject()
                .endObject();
        return builder;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }
}
