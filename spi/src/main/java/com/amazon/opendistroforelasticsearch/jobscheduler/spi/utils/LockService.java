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

package com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils;

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.cronutils.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public final class LockService {
    private static final Logger logger = LogManager.getLogger(LockService.class);
    /**
     * This should go away starting ES 7. We use "_doc" for future compatibility as described here:
     * https://www.elastic.co/guide/en/elasticsearch/reference/6.x/removal-of-types.html#_schedule_for_removal_of_mapping_types
     */
    private static final String MAPPING_TYPE = "_doc";
    private static final String LOCK_INDEX_NAME = ".opendistro-job-scheduler-lock";

    private final Client client;
    private final ClusterService clusterService;

    // This is used in tests to control time.
    private Instant testInstant = null;

    public LockService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    private String lockMapping() {
        try {
            InputStream in = LockService.class.getResourceAsStream("opendistro_job_scheduler_lock.json");
            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line; (line = bufferedReader.readLine()) != null; ) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("Lock Mapping cannot be read correctly.");
        }
    }

    public boolean lockIndexExist() {
        return clusterService.state().routingTable().hasIndex(LOCK_INDEX_NAME);
    }

    @VisibleForTesting
    void createLockIndex(ActionListener<Boolean> listener) {
        if (lockIndexExist()) {
            listener.onResponse(true);
        } else {
            final CreateIndexRequest request = new CreateIndexRequest(LOCK_INDEX_NAME)
                    .mapping(MAPPING_TYPE, lockMapping(), XContentType.JSON);
            client.admin().indices().create(request, ActionListener.wrap(
               response -> listener.onResponse(response.isAcknowledged()),
               exception -> {
                   if (exception instanceof ResourceAlreadyExistsException || exception.getCause() instanceof ResourceAlreadyExistsException) {
                       listener.onResponse(true);
                   } else {
                       listener.onFailure(exception);
                   }
               }
            ));
        }
    }

    /**
     * Attempts to acquire lock the job. If the lock does not exists it attempts to create the lock document.
     * If the Lock document exists, it will try to update and acquire lock.
     *
     * @param jobParameter a {@code ScheduledJobParameter} containing the lock duration.
     * @param context a {@code JobExecutionContext} containing job index name and job id.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the lock if it was acquired
     *                 or else null. Passes {@code IllegalArgumentException} to onFailure if the {@code ScheduledJobParameter} does not
     *                 have {@code LockDurationSeconds}.
     */
    public void acquireLock(final ScheduledJobParameter jobParameter,
                                 final JobExecutionContext context, ActionListener<LockModel> listener) {
        final String jobIndexName = context.getJobIndexName();
        final String jobId = context.getJobId();
        if (jobParameter.getLockDurationSeconds() == null) {
            listener.onFailure(new IllegalArgumentException("Job LockDuration should not be null"));
        } else {
            final long lockDurationSecond = jobParameter.getLockDurationSeconds();
            createLockIndex(ActionListener.wrap(
                    created -> {
                        if (created) {
                            try {
                                findLock(LockModel.generateLockId(jobIndexName, jobId), ActionListener.wrap(
                                        existingLock -> {
                                            if (existingLock != null) {
                                                if (isLockReleasedOrExpired(existingLock)) {
                                                    // Lock is expired. Attempt to acquire lock.
                                                    logger.debug("lock is released or expired: " + existingLock);
                                                    LockModel updateLock = new LockModel(existingLock, getNow(), lockDurationSecond, false);
                                                    updateLock(updateLock, listener);
                                                } else {
                                                    logger.debug("Lock is NOT released or expired. " + existingLock);
                                                    // Lock is still not expired. Return null as we cannot acquire lock.
                                                    listener.onResponse(null);
                                                }
                                            } else {
                                                // There is no lock object and it is first time. Create new lock.
                                                LockModel tempLock = new LockModel(jobIndexName, jobId, getNow(), lockDurationSecond, false);
                                                logger.debug("Lock does not exist. Creating new lock" + tempLock);
                                                createLock(tempLock, listener);
                                            }
                                        },
                                        listener::onFailure
                                ));
                            } catch (VersionConflictEngineException e) {
                                logger.debug("could not acquire lock {}", e.getMessage());
                                listener.onResponse(null);
                            }
                        } else {
                            listener.onResponse(null);
                        }
                    },
                    listener::onFailure
            ));
        }
    }

    private boolean isLockReleasedOrExpired(final LockModel lock) {
        return lock.isReleased() || lock.isExpired();
    }

    private void updateLock(final LockModel updateLock, ActionListener<LockModel> listener) {
        try {
            UpdateRequest updateRequest = new UpdateRequest()
                .index(LOCK_INDEX_NAME)
                .id(updateLock.getLockId())
                .setIfSeqNo(updateLock.getSeqNo())
                .setIfPrimaryTerm(updateLock.getPrimaryTerm())
                .doc(updateLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .fetchSource(true);

            client.update(updateRequest, ActionListener.wrap(
                    response -> listener.onResponse(new LockModel(updateLock, response.getSeqNo(), response.getPrimaryTerm())),
                    exception -> {
                        if (exception instanceof VersionConflictEngineException) {
                            logger.debug("could not acquire lock {}", exception.getMessage());
                        }
                        if (exception instanceof DocumentMissingException) {
                            logger.debug("Document is deleted. This happens if the job is already removed and this is the last run." +
                                    "{}", exception.getMessage());
                        }
                        if (exception instanceof IOException) {
                            logger.error("IOException occurred updating lock.", exception);
                        }
                        listener.onResponse(null);
                    }));
        } catch (IOException e) {
            logger.error("IOException occurred updating lock.", e);
            listener.onResponse(null);
        }
    }

    private void createLock(final LockModel tempLock, ActionListener<LockModel> listener) {
        try {
            final IndexRequest request = new IndexRequest(LOCK_INDEX_NAME)
                .id(tempLock.getLockId())
                .source(tempLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
                .setIfPrimaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                .create(true);
            client.index(request, ActionListener.wrap(
                    response -> listener.onResponse(new LockModel(tempLock, response.getSeqNo(), response.getPrimaryTerm())),
                    exception -> {
                       if (exception instanceof VersionConflictEngineException) {
                           logger.debug("Lock is already created. {}", exception.getMessage());
                       }
                       if (exception instanceof IOException) {
                           logger.error("IOException occurred creating lock", exception);
                       }
                       listener.onResponse(null);
                    }
            ));
        } catch (IOException e) {
            logger.error("IOException occurred creating lock", e);
            listener.onResponse(null);
        }
    }

    private void findLock(final String lockId, ActionListener<LockModel> listener) {
        GetRequest getRequest = new GetRequest(LOCK_INDEX_NAME).id(lockId);
        client.get(getRequest, ActionListener.wrap(
                response -> {
                    if (!response.isExists()) {
                        listener.onResponse(null);
                    } else {
                        try {
                            XContentParser parser = XContentType.JSON.xContent()
                                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                            parser.nextToken();
                            listener.onResponse(LockModel.parse(parser, response.getSeqNo(), response.getPrimaryTerm()));
                        } catch (IOException e) {
                            logger.error("IOException occurred finding lock", e);
                            listener.onResponse(null);
                        }
                    }
                },
                exception -> {
                    logger.error("Exception occurred finding lock", exception);
                    listener.onFailure(exception);
                }
        ));
    }

    /**
     * Attempt to release the lock.
     * Most failure cases are due to {@code lock.seqNo} and {@code lock.primaryTerm} not matching with the existing document.
     *
     * @param lock a {@code LockModel} to be released.
     * @param listener a {@code ActionListener} that has onResponse and onFailure that is used to return whether
     *                 or not the release was successful
     */
    public void release(final LockModel lock, ActionListener<Boolean> listener) {
        if (lock == null) {
            logger.debug("Lock is null. Nothing to release.");
            listener.onResponse(false);
        } else {
            logger.debug("Releasing lock: " + lock);
            final LockModel lockToRelease = new LockModel(lock, true);
            updateLock(lockToRelease, ActionListener.wrap(
                    releasedLock -> listener.onResponse(releasedLock != null),
                    listener::onFailure
            ));
        }
    }

    /**
     * Attempt to delete lock.
     * This should be called as part of clean up when the job for corresponding lock is deleted.
     *
     * @param lockId a {@code String} to be deleted.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return whether
     *                 or not the delete was successful
     */
    public void deleteLock(final String lockId, ActionListener<Boolean> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(LOCK_INDEX_NAME).id(lockId);
        client.delete(deleteRequest, ActionListener.wrap(
                response -> {
                    listener.onResponse(response.getResult() == DocWriteResponse.Result.DELETED ||
                            response.getResult() == DocWriteResponse.Result.NOT_FOUND);
                },
                exception -> {
                    if (exception instanceof IndexNotFoundException || exception.getCause() instanceof IndexNotFoundException) {
                        logger.debug("Index is not found to delete lock. {}", exception.getMessage());
                        listener.onResponse(true);
                    } else {
                        listener.onFailure(exception);
                    }
                }));
    }

    private Instant getNow() {
        return testInstant != null ? testInstant : Instant.now();
    }

    @VisibleForTesting
    void setTime(final Instant testInstant) {
        this.testInstant = testInstant;
    }
}
