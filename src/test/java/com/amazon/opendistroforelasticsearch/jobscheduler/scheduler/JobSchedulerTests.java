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

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes"})
public class JobSchedulerTests {
    @Mock
    private ThreadPool threadPool;

    private JobScheduler scheduler;

    @Before
    public void setup() {
        this.scheduler = new JobScheduler(this.threadPool);
    }

    @Test
    public void testSchedule() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobRunner runner = Mockito.mock(ScheduledJobRunner.class);

        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, true);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));

        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        boolean scheduled = this.scheduler.schedule("index", "job-id", jobParameter, runner);
        Assert.assertTrue(scheduled);
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());

        scheduled = this.scheduler.schedule("index", "job-id", jobParameter, runner);
        Assert.assertTrue(scheduled);
        // already scheduled, no extra threadpool call
        Mockito.verify(this.threadPool, Mockito.times(1)).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testSchedule_disabledJob() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(),
                new CronSchedule("* * * * *", ZoneId.systemDefault()), false);
        boolean scheduled = this.scheduler.schedule("index-name", "job-id", jobParameter, null);
        Assert.assertFalse(scheduled);
    }

    @Test
    public void testDeschedule_singleJob() {
        JobSchedulingInfo jobInfo = new JobSchedulingInfo("job-id", null);
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

    @Test
    public void testDeschedule_bulk() {
        Assert.assertTrue(this.scheduler.bulkDeschedule("index-name", null).isEmpty());

        JobSchedulingInfo jobInfo1 = new JobSchedulingInfo("job-id-1", null);
        Scheduler.ScheduledCancellable scheduledCancellable1 = Mockito.mock(Scheduler.ScheduledCancellable.class);
        jobInfo1.setScheduledCancellable(scheduledCancellable1);
        Mockito.when(scheduledCancellable1.cancel()).thenReturn(false);
        this.scheduler.getScheduledJobInfo().addJob("index-name", "job-id-1", jobInfo1);

        JobSchedulingInfo jobInfo2 = new JobSchedulingInfo("job-id-2", null);
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

    @Test
    public void testDeschedule_noSuchJob() {
        Assert.assertTrue(this.scheduler.deschedule("index-name", "job-id"));
    }

    @Test
    public void testReschedule_noEnableTime() {
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                null, null, null, false);
        Assert.assertFalse(this.scheduler.reschedule(jobParameter, null, null));
    }

    @Test
    public void testReschedule_jobDescheduled() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, false);
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(true);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));

        Assert.assertFalse(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null));
    }

    @Test
    public void testReschedule_scheduleJob() {
        Schedule schedule = Mockito.mock(Schedule.class);
        ScheduledJobParameter jobParameter = buildScheduledJobParameter("job-id", "dummy job name",
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), schedule, false);
        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo("job-id", jobParameter);
        Instant now = Instant.now();
        jobSchedulingInfo.setDescheduled(false);

        Mockito.when(schedule.getNextExecutionTime(Mockito.any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));
        Scheduler.ScheduledCancellable cancellable = Mockito.mock(Scheduler.ScheduledCancellable.class);
        Mockito.when(this.threadPool.schedule(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        Assert.assertTrue(this.scheduler.reschedule(jobParameter, jobSchedulingInfo, null));
        Assert.assertEquals(cancellable, jobSchedulingInfo.getScheduledCancellable());
        Mockito.verify(this.threadPool).schedule(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    static ScheduledJobParameter buildScheduledJobParameter(String id, String name, Instant updateTime,
            Instant enableTime, Schedule schedule, boolean enabled) {
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
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };
    }

}
