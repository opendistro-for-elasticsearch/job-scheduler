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

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobDocVersion;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(RandomizedRunner.class)
@SuppressWarnings({"rawtypes"})
public class JobSchedulerTests extends ESTestCase {
    private ThreadPool threadPool;

    private JobScheduler scheduler;

    private JobDocVersion dummyVersion = new JobDocVersion(1L, 1L, 1L);
    private Double jitterLimit = 0.95;

    @Before
    public void setup() {
        this.threadPool = Mockito.mock(ThreadPool.class);
        this.scheduler = new JobScheduler(this.threadPool, null);
    }

    public void testSchedule() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);

        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, true);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);


        boolean scheduled = this.scheduler.schedule("index", "job-id", jobParameter, runner, dummyVersion, jitterLimit);
        Assert.assertTrue(scheduled);
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());

        scheduled = this.scheduler.schedule("index", "job-id", jobParameter, runner, dummyVersion, jitterLimit);
        Assert.assertTrue(scheduled);
        // already scheduled, no extra threadpool call
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testSchedule_disabledJob() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(),
                new CronSchedule("* * * * *", ZoneId.systemDefault()), false);
        boolean scheduled = this.scheduler.schedule("index-name", "job-id", jobParameter, null, dummyVersion, jitterLimit);
        Assert.assertFalse(scheduled);
    }

    public void testDeschedule_singleJob() {
        JobSchedulingInfo jobInfo = new JobSchedulingInfo("job-index", "job-id", null);
        Scheduler.ScheduledCancellable scheduledCancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo.setScheduledCancellable(scheduledCancellable);
        Mockito.when(scheduledCancellable.cancel()).thenReturn(false);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id", jobInfo);

        // test future.cancel return false
        boolean descheduled = this.scheduler.deschedule("index-name", "job-id");
        Assert.assertFalse(descheduled);
        Mockito.verify(scheduledCancellable).cancel();
        Assert.assertFalse(this.scheduler.getScheduledJobInfo().getJobsByIndex("index-name").isEmpty());

        // test future.cancel return true
        Mockito.when(scheduledCancellable.cancel()).thenReturn(true);
        descheduled = this.scheduler.deschedule("index-name", "job-id");
        Assert.assertTrue(descheduled);
        Mockito.verify(scheduledCancellable, Mockito.times(2)).cancel();
        Assert.assertTrue(this.scheduler.getScheduledJobInfo().getJobsByIndex("index-name").isEmpty());
    }

    public void testDeschedule_bulk() {
        Assert.assertTrue(this.scheduler.bulkDeschedule("index-name", null).isEmpty());

        JobSchedulingInfo jobInfo1 = new JobSchedulingInfo("job-index", "job-id-1", null);
        Scheduler.ScheduledCancellable scheduledCancellable1 = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo1.setScheduledCancellable(scheduledCancellable1);
        Mockito.when(scheduledCancellable1.cancel()).thenReturn(false);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id-1", jobInfo1);

        JobSchedulingInfo jobInfo2 = new JobSchedulingInfo("job-index", "job-id-2", null);
        Scheduler.ScheduledCancellable scheduledCancellable2 = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo2.setScheduledCancellable(scheduledCancellable2);
        Mockito.when(scheduledCancellable2.cancel()).thenReturn(true);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id-2", jobInfo2);

        List<String> ids = new ArrayList<>();
        ids.add("job-id-1");
        ids.add("job-id-2");

        List<String> result = this.scheduler.bulkDeschedule("index-name", ids);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains("job-id-1"));
        Mockito.verify(scheduledCancellable1).cancel();
        Mockito.verify(scheduledCancellable2).cancel();
    }

    public void testDeschedule_noSuchJob() {
        Assert.assertTrue(this.scheduler.deschedule("index-name", "job-id"));
    }

    public void testReschedule_noEnableTime() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                null, null, null, false);
        Assert.assertFalse(this.scheduler.reschedule(jobParameter, null, null, dummyVersion, jitterLimit));
    }

    public void testReschedule_jobDescheduled() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, false, 0.6);
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-index", "job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(true);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
            .thenReturn(now.plus(1, ChronoUnit.MINUTES))
            .thenReturn(now.plus(2, ChronoUnit.MINUTES));

        Assert.assertFalse(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
    }

    public void testReschedule_scheduleJob() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, false, 0.6);
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-index", "job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(false);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
            .thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES))
            .thenReturn(Instant.now().plus(2, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        Assert.assertTrue(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
        Assert.assertEquals(cancellable, jobSchedulingInfo.getScheduledCancellable());
        Mockito.verify(this.threadPool).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testReschedule_scheduleJobwithDelay() {
        final long delaySeconds = 90;

        Schedule schedule = Mockito.mock(Schedule.class);
        // Create a ScheduledJobParameter with delay
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, false, delaySeconds);
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-index", "job-id", jobParameter);
        jobSchedulingInfo.setDescheduled(false);

        // Mock the time interval for the job executions.
        // The interval for the 1st and 2nd executions is 2 minutes, and 1 minutes for the 3rd and 4th executions.
        Instant now = Instant.now();
        Mockito.when(schedule.getNextExecutionTime(Mockito.any()))
                .thenReturn(now.plus(1, ChronoUnit.MINUTES))
                .thenReturn(now.plus(3, ChronoUnit.MINUTES))
                .thenReturn(now.plus(1, ChronoUnit.MINUTES))
                .thenReturn(now.plus(2, ChronoUnit.MINUTES));

        // Set a fixed Clock object that returns the same instant
        this.scheduler.setClock(Clock.fixed(now, ZoneId.systemDefault()));
        // Calculate the expected duration from 'now' to the next execution time with delay
        long durationSeconds = 60 + delaySeconds;

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        Assert.assertTrue(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
        Assert.assertEquals(cancellable, jobSchedulingInfo.getScheduledCancellable());
        // Verify the ThreadPool.schedule() is called, and the total delay time for the job is correct
        TimeValue expectedDuration = new TimeValue(durationSeconds, TimeUnit.SECONDS);
        Mockito.verify(this.threadPool).schedule(Mockito.any(), Mockito.eq(expectedDuration), Mockito.anyString());

        // Calculate the expected duration from 'now' to the next execution time with delay.
        // The delay exceeds the time interval between the 3rd and 4th executions,
        // so it is expected to be cut down to the same with the interval.
        long shortenedDurationSeconds = 60 + 60;
        Assert.assertTrue(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null, dummyVersion, jitterLimit));
        // Verify the ThreadPool.schedule() is called, and the total delay time for the job is correct
        TimeValue expectedShortenedDuration = new TimeValue(shortenedDurationSeconds, TimeUnit.SECONDS);
        Mockito.verify(this.threadPool).schedule(Mockito.any(), Mockito.eq(expectedShortenedDuration), Mockito.anyString());
    }

    static ScheduledJobParameter buildScheduledJobParameter(String id, String name, Instant updateTime,
        Instant enableTime, Schedule schedule, boolean enabled) {
        return buildScheduledJobParameter(id, name, updateTime, enableTime, schedule, enabled, null, null);
    }

    /**
     * Use this constructor to build a ScheduledJobParameter with a jitter factor.
     *
     * @param jitter a percentage which indicates the inconsistent delay
     */
    static ScheduledJobParameter buildScheduledJobParameter(String id, String name, Instant updateTime,
                                                            Instant enableTime, Schedule schedule, boolean enabled, Double jitter) {
        return buildScheduledJobParameter(id, name, updateTime, enableTime, schedule, enabled, jitter, null);
    }

    /**
     * Use this constructor to build a ScheduledJobParameter with a fixed delay.
     *
     * @param delaySeconds a fixed delay in seconds
     */
    static ScheduledJobParameter buildScheduledJobParameter(String id, String name, Instant updateTime,
                                                            Instant enableTime, Schedule schedule, boolean enabled, Long delaySeconds) {
        return buildScheduledJobParameter(id, name, updateTime, enableTime, schedule, enabled, null, delaySeconds);
    }

    static ScheduledJobParameter buildScheduledJobParameter(String id, String name, Instant updateTime,
            Instant enableTime, Schedule schedule, boolean enabled, Double jitter, Long delaySeconds) {
        return new ScheduledJobParameter() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public Instant getLastUpdateTime() {
                return updateTime;
            }

            @Override
            public Instant getEnabledTime() {
                return enableTime;
            }

            @Override
            public Schedule getSchedule() {
                return schedule;
            }

            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public Double getJitter() {
                return jitter;
            }

            @Override
            public Long getDelaySeconds() {
                return delaySeconds;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };
    }

}
