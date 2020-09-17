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

import com.cronutils.model.time.ExecutionTime;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class CronScheduleTests extends ESTestCase {

    private CronSchedule cronSchedule;

    @Before
    public void setup() {
        this.cronSchedule = new CronSchedule("* * * * *", ZoneId.systemDefault());
    }

    public void testNextTimeToExecute() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Instant nextMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60 + 60);

        Duration expected = Duration.between(now, nextMinute);
        Duration duration = this.cronSchedule.nextTimeToExecute();

        Assert.assertEquals(expected, duration);
    }

    public void testGetPeriodStartingAt() {
        Instant now = Instant.now();
        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);
        Instant nextMinute = currentMinute.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.cronSchedule.getPeriodStartingAt(currentMinute);

        Assert.assertEquals(currentMinute, period.v1());
        Assert.assertEquals(nextMinute, period.v2());
    }

    public void testGetPeriodStartingAt_nullStartTime() {
        Instant now = Instant.now();

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);
        Instant nextMinute = currentMinute.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.cronSchedule.getPeriodStartingAt(null);

        Assert.assertEquals(currentMinute, period.v1());
        Assert.assertEquals(nextMinute, period.v2());
    }

    public void testGetPeriodStartingAt_noLastExecution() {
        ExecutionTime mockExecution = Mockito.mock(ExecutionTime.class);
        this.cronSchedule.setExecutionTime(mockExecution);
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Mockito.when(mockExecution.lastExecution(ZonedDateTime.ofInstant(now, ZoneId.systemDefault()))).thenReturn(Optional.empty());

        Tuple<Instant, Instant> period = this.cronSchedule.getPeriodStartingAt(null);

        Assert.assertEquals(now, period.v1());
        Assert.assertEquals(now, period.v2());
        Mockito.verify(mockExecution).lastExecution(ZonedDateTime.ofInstant(now, ZoneId.systemDefault()));
    }

    public void testRunningOnTime() {
        Instant now = Instant.now();

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);

        Instant lastExecutionTime = currentMinute.minus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.cronSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.plus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.cronSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.plus(10, ChronoUnit.SECONDS);
        Assert.assertFalse(this.cronSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(10, ChronoUnit.SECONDS);
        Assert.assertFalse(this.cronSchedule.runningOnTime(lastExecutionTime));
    }

    public void testRunningOnTime_nullParam() {
        Assert.assertTrue(this.cronSchedule.runningOnTime(null));
    }

    public void testRunningOnTime_noLastExecution() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);
        ExecutionTime mockExecutionTime = Mockito.mock(ExecutionTime.class);
        this.cronSchedule.setExecutionTime(mockExecutionTime);

        Mockito.when(mockExecutionTime.lastExecution(ZonedDateTime.ofInstant(now, ZoneId.systemDefault())))
                .thenReturn(Optional.empty());

        Assert.assertFalse(this.cronSchedule.runningOnTime(now));
    }

    public void testToXContent() throws IOException {
        CronSchedule schedule = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"));
        String expectedJsonStr = "{\"cron\":{\"expression\":\"* * * * *\",\"timezone\":\"PST8PDT\"}}";
        Assert.assertEquals(expectedJsonStr,
                XContentHelper.toXContent(schedule, XContentType.JSON, false).utf8ToString());
    }

    public void testCronScheduleEqualsAndHashCode() {
        CronSchedule cronScheduleOne = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"));
        CronSchedule cronScheduleTwo = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"));
        CronSchedule cronScheduleThree = new CronSchedule("1 * * * *", ZoneId.of("PST8PDT"));

        Assert.assertEquals(cronScheduleOne, cronScheduleTwo);
        Assert.assertNotEquals(cronScheduleOne, cronScheduleThree);
        Assert.assertEquals(cronScheduleOne.hashCode(), cronScheduleTwo.hashCode());
    }

    public void testCronScheduleAsStream() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        cronSchedule.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        CronSchedule newCronSchedule = new CronSchedule(input);
        assertEquals(cronSchedule, newCronSchedule);
    }
}
