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

package com.amazon.opendistroforelasticsearch.jobscheduler.utils;

import com.amazon.opendistroforelasticsearch.jobscheduler.model.lock.LockModel;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class LockServiceTests extends ESIntegTestCase {

    static final String JOB_ID = "test_job_id";
    static final String JOB_TYPE = "test_job_type";
    static final long LOCK_DURATION_SECONDS = 60;

    @Test
    public void sanityTest() {
        LockService lockService = new LockService(client(), clusterService());
        LockModel lock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull("Expected to successfully grab lock", lock);
        assertEquals(lock.getJobId(), JOB_ID);
        assertEquals(lock.getJobType(), JOB_TYPE);
        assertEquals(lock.getLockId(), LockModel.generateLockId(JOB_TYPE, JOB_ID));
        assertEquals(lock.getLockDurationSeconds(), LOCK_DURATION_SECONDS);
        assertTrue("Failed to release lock.", lockService.release(lock));
    }

    @Test
    public void secondAcquireLockFail() {
        LockService lockService = new LockService(client(), clusterService());
        LockModel lock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        LockModel lock2 = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull("Expected to successfully grab lock", lock);
        assertNull("Expected to failed to get lock.", lock2);
        assertTrue(lockService.release(lock));
    }

    @Test
    public void lockReleasedAndAcquired() {
        LockService lockService = new LockService(client(), clusterService());
        LockModel lock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull("Expected to successfully grab lock", lock);
        assertTrue("Failed to release lock.", lockService.release(lock));
        LockModel lock2 = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull("Expected to successfully grab lock", lock2);
        assertTrue("Failed to release lock.", lockService.release(lock2));
    }

    @Test
    public void lockExpired() {
        LockService lockService = new LockService(client(), clusterService());
        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        LockModel lock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull("Expected to successfully grab lock", lock);
        // Set lock back to current time to make the lock expire.
        lockService.setTime(null);
        LockModel lock2 = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull("Expected to successfully grab lock", lock2);
        assertFalse("Expected to fail releasing lock.", lockService.release(lock));
        assertTrue("Expecting to successfully release lock.", lockService.release(lock2));
    }

    @Test
    public void multiThreadCreateLock() throws Exception {
        final LockService lockService = new LockService(client(), clusterService());
        lockService.createLockIndex();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
        Callable<Integer> callable = () -> {
            LockModel lock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
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

        final LockModel lockModel = lockModelAtomicReference.get();
        assertNotNull(lockModel);
        assertTrue(lockService.release(lockModel));
    }

    @Test
    public void multiThreadAcquireLock() throws Exception {
        final LockService lockService = new LockService(client(), clusterService());
        lockService.createLockIndex();

        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        LockModel createdLock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
        assertNotNull(createdLock);
        // Set lock back to current time to make the lock expire.
        lockService.setTime(null);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
        Callable<Integer> callable = () -> {
            LockModel lock = lockService.acquireLock(JOB_TYPE, JOB_ID, LOCK_DURATION_SECONDS);
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

        final LockModel lockModel = lockModelAtomicReference.get();
        assertNotNull(lockModel);
        assertTrue(lockService.release(lockModel));
    }
}
