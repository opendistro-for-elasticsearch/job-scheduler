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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class IntervalScheduleTests extends ESTestCase {

    private IntervalSchedule intervalSchedule;
    private Instant startTime;

    @Before
    public void setup() throws ParseException {
        startTime = new SimpleDateFormat("MM/dd/yyyy").parse("01/01/2019").toInstant();
        this.intervalSchedule = new IntervalSchedule(startTime, 1, ChronoUnit.MINUTES);
    }

    @Test (expected =  IllegalArgumentException.class)
    public void testConstructor_notSupportedTimeUnit() throws ParseException {
        Instant startTime = new SimpleDateFormat("MM/dd/yyyy").parse("01/01/2019").toInstant();
        new IntervalSchedule(startTime, 1, ChronoUnit.MILLIS);
    }

    public void testNextTimeToExecution() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        Instant nextMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60 + 60);
        Duration expected = Duration.of(nextMinute.toEpochMilli() - now.toEpochMilli(), ChronoUnit.MILLIS);

        Assert.assertEquals(expected, this.intervalSchedule.nextTimeToExecute());
    }

    public void testGetPeriodStartingAt() {
        Instant now = Instant.now();
        Instant oneMinLater = now.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.intervalSchedule.getPeriodStartingAt(now);

        Assert.assertEquals(now, period.v1());
        Assert.assertEquals(oneMinLater, period.v2());
    }

    public void testGetPeriodStartingAt_nullParam() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        Instant oneMinLater = now.plus(1L, ChronoUnit.MINUTES);
        Tuple<Instant, Instant> period = this.intervalSchedule.getPeriodStartingAt(null);

        Assert.assertEquals(now, period.v1());
        Assert.assertEquals(oneMinLater, period.v2());
    }

    public void testRunningOnTime() {
        Instant now = Instant.now();
        if(now.toEpochMilli() % (60 * 1000) == 0) {
            // test "now" is not execution time case
            now = now.plus(10, ChronoUnit.SECONDS);
        }
        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        Instant lastExecutionTime = currentMinute.plus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.intervalSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(2, ChronoUnit.SECONDS);
        Assert.assertFalse(this.intervalSchedule.runningOnTime(lastExecutionTime));

        // test "now" is execution time case
        now = currentMinute;
        testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        lastExecutionTime = currentMinute.minus(59500, ChronoUnit.MILLIS);
        Assert.assertTrue(this.intervalSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(58500, ChronoUnit.MILLIS);
        Assert.assertFalse(this.intervalSchedule.runningOnTime(lastExecutionTime));

        // test "now" is in the first second, and last run in current second of current second
        now = currentMinute.plus(500, ChronoUnit.MILLIS);
        testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        lastExecutionTime = currentMinute;
        Assert.assertTrue(this.intervalSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(2, ChronoUnit.SECONDS);
        Assert.assertFalse(this.intervalSchedule.runningOnTime(lastExecutionTime));
    }

    public void testRunningOnTime_nullLastExetime() {
        Assert.assertTrue(this.intervalSchedule.runningOnTime(null));
    }

    public void testToXContent() throws IOException {
        long epochMillis = this.startTime.toEpochMilli();
        String xContentJsonStr = "{\"interval\":{\"start_time\":" + epochMillis + ",\"period\":1,\"unit\":\"Minutes\"}}";
                XContentHelper.toXContent(this.intervalSchedule, XContentType.JSON, false)
                .utf8ToString();
        Assert.assertEquals(xContentJsonStr, XContentHelper.toXContent(this.intervalSchedule, XContentType.JSON, false)
                .utf8ToString());
    }

    public void testIntervalScheduleEqualsAndHashCode() {
        Long epochMilli = Instant.now().toEpochMilli();
        IntervalSchedule intervalScheduleOne = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 5, ChronoUnit.MINUTES);
        IntervalSchedule intervalScheduleTwo = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 5, ChronoUnit.MINUTES);
        IntervalSchedule intervalScheduleThree = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 4, ChronoUnit.MINUTES);

        Assert.assertEquals(intervalScheduleOne, intervalScheduleTwo);
        Assert.assertNotEquals(intervalScheduleOne, intervalScheduleThree);
        Assert.assertEquals(intervalScheduleOne.hashCode(), intervalScheduleTwo.hashCode());
    }

    public void testIntervalScheduleAsStream() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        intervalSchedule.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        IntervalSchedule newIntervalSchedule = new IntervalSchedule(input);
        assertEquals(intervalSchedule, newIntervalSchedule);
    }
}
