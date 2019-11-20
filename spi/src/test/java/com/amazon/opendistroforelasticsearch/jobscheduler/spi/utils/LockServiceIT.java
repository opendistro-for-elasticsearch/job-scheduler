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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    public void testSanity() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);
        Instant testTime = Instant.now();
        lockService.setTime(testTime);
        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock.", lock);
                    assertEquals("job_id does not match.", JOB_ID, lock.getJobId());
                    assertEquals("job_index_name does not match.", JOB_INDEX_NAME, lock.getJobIndexName());
                    assertEquals("lock_id does not match.", LockModel.generateLockId(JOB_INDEX_NAME, JOB_ID), lock.getLockId());
                    assertEquals("lock_duration_seconds does not match.", LOCK_DURATION_SECONDS, lock.getLockDurationSeconds());
                    assertEquals("lock_time does not match.", testTime.getEpochSecond(), lock.getLockTime().getEpochSecond());
                    assertFalse("Lock should not be released.", lock.isReleased());
                    assertFalse("Lock should not expire.", lock.isExpired());
                    lockService.release(lock, ActionListener.wrap(
                            released -> {
                                assertTrue("Failed to release lock.", released);
                                lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                        deleted -> {
                                            assertTrue("Failed to delete lock.", deleted);
                                            latch.countDown();
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10000L, TimeUnit.MILLISECONDS);
    }

    public void testSecondAcquireLockFail() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                            lock2 -> {
                                assertNull("Expected to failed to get lock.", lock2);
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertTrue("Failed to release lock.", released);
                                            lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                    deleted -> {
                                                        assertTrue("Failed to delete lock.", deleted);
                                                        latch.countDown();
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10000L, TimeUnit.MILLISECONDS);
    }

    public void testLockReleasedAndAcquired() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    lockService.release(lock, ActionListener.wrap(
                            released -> {
                                assertTrue("Failed to release lock.", released);
                                lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                        lock2 -> {
                                            assertNotNull("Expected to successfully grab lock2", lock2);
                                            lockService.release(lock2, ActionListener.wrap(
                                                    released2 -> {
                                                        assertTrue("Failed to release lock2.", released2);
                                                        lockService.deleteLock(lock2.getLockId(), ActionListener.wrap(
                                                                deleted -> {
                                                                    assertTrue("Failed to delete lock2.", deleted);
                                                                    latch.countDown();
                                                                },
                                                                exception -> fail(exception.getMessage())
                                                        ));
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10000L, TimeUnit.MILLISECONDS);
    }

    public void testLockExpired() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), clusterService());
        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);


        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    // Set lock back to current time to make the lock expire.
                    lockService.setTime(null);
                    lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                            lock2 -> {
                                assertNotNull("Expected to successfully grab lock", lock2);
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertFalse("Expected to fail releasing lock.", released);
                                            lockService.release(lock2, ActionListener.wrap(
                                                    released2 -> {
                                                        assertTrue("Expecting to successfully release lock.", released2);
                                                        lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                                deleted -> {
                                                                    assertTrue("Failed to delete lock.", deleted);
                                                                    latch.countDown();
                                                                },
                                                                exception -> fail(exception.getMessage())
                                                        ));
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10000L, TimeUnit.MILLISECONDS);
    }

    public void testDeleteLockWithOutIndexCreation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), clusterService());
        lockService.deleteLock("NonExistingLockId", ActionListener.wrap(
                deleted -> {
                    assertTrue("Failed to delete lock.", deleted);
                    latch.countDown();
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10000L, TimeUnit.MILLISECONDS);
    }

    public void testDeleteNonExistingLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), clusterService());
        lockService.createLockIndex(ActionListener.wrap(
                created -> {
                    if (created) {
                        lockService.deleteLock("NonExistingLockId", ActionListener.wrap(
                                deleted -> {
                                    assertTrue("Failed to delete lock.", deleted);
                                    latch.countDown();
                                },
                                exception -> fail(exception.getMessage())
                        ));

                    } else {
                        fail("Failed to create lock index.");
                    }
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10000L, TimeUnit.MILLISECONDS);
    }

    private volatile static AtomicInteger multiThreadCreateLockCounter = new AtomicInteger(0);

    public void testMultiThreadCreateLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);


        lockService.createLockIndex(ActionListener.wrap(
                created -> {
                    if (created) {
                        ExecutorService executor = Executors.newFixedThreadPool(3);
                        final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
                        Callable<Boolean> callable = () -> {
                            CountDownLatch callableLatch = new CountDownLatch(1);
                            lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                    lock -> {
                                        if (lock != null) {
                                            lockModelAtomicReference.set(lock);
                                            multiThreadCreateLockCounter.getAndAdd(1);
                                        }
                                        callableLatch.countDown();
                                    },
                                    exception -> fail(exception.getMessage())
                            ));
                            callableLatch.await(10000L, TimeUnit.MILLISECONDS);
                            return true;
                        };

                        List<Callable<Boolean>> callables = Arrays.asList(
                                callable,
                                callable,
                                callable
                        );

                        executor.invokeAll(callables)
                                .forEach(future -> {
                                    try {
                                        future.get();
                                    } catch (Exception e) {
                                        fail(e.getMessage());
                                    }
                                });
                        executor.shutdown();
                        executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);

                        assertEquals("There should be only one that grabs the lock.", 1, multiThreadCreateLockCounter.get());

                        final LockModel lock = lockModelAtomicReference.get();
                        assertNotNull("Expected to successfully grab lock", lock);
                        lockService.release(lock, ActionListener.wrap(
                                released -> {
                                    assertTrue("Failed to release lock.", released);
                                    lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                            deleted -> {
                                                assertTrue("Failed to delete lock.", deleted);
                                                latch.countDown();
                                            },
                                            exception -> fail(exception.getMessage())
                                    ));
                                },
                                exception -> fail(exception.getMessage())
                        ));
                    } else {
                        fail("Failed to create lock index.");
                    }
                },
                exception -> fail(exception.getMessage())
        ));
        assertTrue("Test timed out - possibly leaked into other tests", latch.await(30000L, TimeUnit.MILLISECONDS));
    }

    private volatile static AtomicInteger multiThreadAcquireLockCounter = new AtomicInteger(0);

    public void testMultiThreadAcquireLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final LockService lockService = new LockService(client(), clusterService());
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
            lockService, JOB_INDEX_NAME, JOB_ID);

        lockService.createLockIndex(ActionListener.wrap(
                created -> {
                    if (created) {
                        // Set lock time in the past.
                        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
                        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                createdLock -> {
                                    assertNotNull(createdLock);
                                    // Set lock back to current time to make the lock expire.
                                    lockService.setTime(null);

                                    ExecutorService executor = Executors.newFixedThreadPool(3);
                                    final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
                                    Callable<Boolean> callable = () -> {
                                        CountDownLatch callableLatch = new CountDownLatch(1);
                                        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                                lock -> {
                                                    if (lock != null) {
                                                        lockModelAtomicReference.set(lock);
                                                        Integer test = multiThreadAcquireLockCounter.getAndAdd(1);
                                                    }
                                                    callableLatch.countDown();
                                                },
                                                exception -> fail(exception.getMessage())
                                        ));
                                        callableLatch.await(10000L, TimeUnit.MILLISECONDS);
                                        return true;
                                    };

                                    List<Callable<Boolean>> callables = Arrays.asList(
                                            callable,
                                            callable,
                                            callable
                                    );

                                    executor.invokeAll(callables);
                                    executor.shutdown();
                                    executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);

                                    assertEquals("There should be only one that grabs the lock.", 1, multiThreadAcquireLockCounter.get());

                                    final LockModel lock = lockModelAtomicReference.get();
                                    assertNotNull("Expected to successfully grab lock", lock);
                                    lockService.release(lock, ActionListener.wrap(
                                            released -> {
                                                assertTrue("Failed to release lock.", released);
                                                lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                        deleted -> {
                                                            assertTrue("Failed to delete lock.", deleted);
                                                            latch.countDown();
                                                        },
                                                        exception -> fail(exception.getMessage())
                                                ));
                                            },
                                            exception -> fail(exception.getMessage())
                                    ));
                                },
                                exception -> fail(exception.getMessage())
                        ));
                    } else {
                        fail("Failed to create lock index.");
                    }
                },
                exception -> fail(exception.getMessage())
        ));
        assertTrue("Test timed out - possibly leaked into other tests", latch.await(30000L, TimeUnit.MILLISECONDS));
    }
}
