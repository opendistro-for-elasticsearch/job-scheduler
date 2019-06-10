/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils;

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobDocVersion;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes =1)
public class LockServiceIT extends ESIntegTestCase {

    static final String JOB_ID = "test_job_id";
    static final String JOB_INDEX_NAME = "test_job_index_name";
    static final long LOCK_DURATION_SECONDS = 60;
    static final ScheduledJobParameter  TEST_SCHEDULED_JOB_PARAM = new ScheduledJobParameter() {

        @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder;
        }

        @Override public String getName() {
            return null;
        }

        @Override public Instant getLastUpdateTime() {
            return null;
        }

        @Override public Instant getEnabledTime() {
            return null;
        }

        @Override public Schedule getSchedule() {
            return null;
        }

        @Override public boolean isEnabled() {
            return false;
        }

        @Override public Long getLockDurationSeconds() {
            return LOCK_DURATION_SECONDS;
        }
    };

    public void testSanity() {
        LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        Instant testTime = Instant.now();
        lockService.setTime(testTime);
        LockModel lock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull("Expected to successfully grab lock.", lock);
        assertEquals("job_id does not match.", JOB_ID, lock.getJobId());
        assertEquals("job_index_name does not match.", JOB_INDEX_NAME, lock.getJobIndexName());
        assertEquals("lock_id does not match.", LockModel.generateLockId(JOB_INDEX_NAME, JOB_ID), lock.getLockId());
        assertEquals("lock_duration_seconds does not match.", LOCK_DURATION_SECONDS, lock.getLockDurationSeconds());
        assertEquals("lock_time does not match.", testTime.getEpochSecond(), lock.getLockTime().getEpochSecond());
        assertFalse("Lock should not be released.", lock.isReleased());
        assertFalse("Lock should not expire.", lock.isExpired());
        assertTrue("Failed to release lock.", lockService.release(lock));
        assertTrue("Failed to delete lock.",lockService.deleteLock(lock.getLockId()));
    }

    public void testSecondAcquireLockFail() {
        LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        LockModel lock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        LockModel lock2 = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull("Expected to successfully grab lock", lock);
        assertNull("Expected to failed to get lock.", lock2);
        assertTrue("Failed to release lock.", lockService.release(lock));
        lockService.deleteLock(lock.getLockId());
        assertTrue("Failed to delete lock.",lockService.deleteLock(lock.getLockId()));
    }

    public void testLockReleasedAndAcquired() {
        LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        LockModel lock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull("Expected to successfully grab lock", lock);
        assertTrue("Failed to release lock.", lockService.release(lock));
        LockModel lock2 = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull("Expected to successfully grab lock", lock2);
        assertTrue("Failed to release lock.", lockService.release(lock2));
        assertTrue("Failed to delete lock.",lockService.deleteLock(lock.getLockId()));
    }

    public void testLockExpired() {
        LockService lockService = new LockService(client(), clusterService());
        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);


        LockModel lock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull("Expected to successfully grab lock", lock);
        // Set lock back to current time to make the lock expire.
        lockService.setTime(null);
        LockModel lock2 = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull("Expected to successfully grab lock", lock2);
        assertFalse("Expected to fail releasing lock.", lockService.release(lock));
        assertTrue("Expecting to successfully release lock.", lockService.release(lock2));
        assertTrue("Failed to delete lock.",lockService.deleteLock(lock.getLockId()));
    }

    public void testDeleteLockWithOutIndexCreation() {
        LockService lockService = new LockService(client(), clusterService());
        assertTrue("Failed to delete lock.",lockService.deleteLock("NonExistingLockId"));
    }

    public void testDeleteNonExistingLock() {
        LockService lockService = new LockService(client(), clusterService());
        lockService.createLockIndex();
        assertTrue("Failed to delete lock.",lockService.deleteLock("NonExistingLockId"));
    }

    public void testMultiThreadCreateLock() throws Exception {
        final LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        lockService.createLockIndex();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
        Callable<Integer> callable = () -> {
            LockModel lock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
            if (lock != null) {
                lockModelAtomicReference.set(lock);
                return 1;
            }
            return 0;
        };

        List<Callable<Integer>> callables = Arrays.asList(
            callable,
            callable,
            callable
        );

        final int counter = executor.invokeAll(callables)
            .stream()
            .map(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            })
            .mapToInt(Integer::intValue)
            .sum();
        executor.shutdown();

        assertEquals("There should be only one that grabs the lock.", 1, counter);

        final LockModel lock = lockModelAtomicReference.get();
        assertNotNull("Expected to successfully grab lock", lock);
        assertTrue("Failed to release lock.", lockService.release(lock));
        assertTrue("Failed to delete lock.",lockService.deleteLock(lock.getLockId()));
    }

    public void testMultiThreadAcquireLock() throws Exception {
        final LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        lockService.createLockIndex();

        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        LockModel createdLock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
        assertNotNull(createdLock);
        // Set lock back to current time to make the lock expire.
        lockService.setTime(null);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
        Callable<Integer> callable = () -> {
            LockModel lock = lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context);
            if (lock != null) {
                lockModelAtomicReference.set(lock);
                return 1;
            }
            return 0;
        };

        List<Callable<Integer>> callables = Arrays.asList(
            callable,
            callable,
            callable
        );

        final int counter = executor.invokeAll(callables)
            .stream()
            .map(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            })
            .mapToInt(Integer::intValue)
            .sum();

        executor.shutdown();

        assertEquals("There should be only one that grabs the lock.", 1, counter);

        final LockModel lock = lockModelAtomicReference.get();
        assertNotNull("Expected to successfully grab lock", lock);
        assertTrue("Failed to release lock.", lockService.release(lock));
        assertTrue("Failed to delete lock.",lockService.deleteLock(lock.getLockId()));
    }
}
