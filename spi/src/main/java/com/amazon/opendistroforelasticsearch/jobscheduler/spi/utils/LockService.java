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

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.cronutils.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
    private boolean isIndexInitialized;

    // This is used in tests to control time.
    private Instant testInstant = null;

    public LockService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        this.isIndexInitialized = false;
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

    @VisibleForTesting
    boolean createLockIndex() {
        final boolean exists = clusterService.state().routingTable().hasIndex(LOCK_INDEX_NAME);
        if (exists) {
            return true;
        }

        try {
            final CreateIndexRequest request = new CreateIndexRequest(LOCK_INDEX_NAME)
                .mapping(MAPPING_TYPE, lockMapping(), XContentType.JSON);
            return client.admin().indices().create(request).actionGet().isAcknowledged();
        } catch (ResourceAlreadyExistsException e) {
            return true;
        }
    }

    public LockModel acquireLock(final String jobIndexName, final String jobId, final long lockDurationSecond) {
        if (!isIndexInitialized) {
            isIndexInitialized = createLockIndex();
        }
        try {
            LockModel existingLock = findLock(LockModel.generateLockId(jobIndexName, jobId));
            if (existingLock != null) {
                if (isLockReleasedOrExpired(existingLock)) {
                    // Lock is expired. Attempt to acquire lock.
                    logger.debug("lock is released or expired: " + existingLock);
                    LockModel updateLock = new LockModel(existingLock, getNow(), lockDurationSecond, false);
                    return updateLock(updateLock);
                } else {
                    logger.debug("Lock is NOT released or expired. " + existingLock);
                    // Lock is still not expired. Return null as we cannot acquire lock.
                    return null;
                }
            } else {
                // There is no lock object and it is first time. Create new lock.
                LockModel tempLock = new LockModel(jobIndexName, jobId, getNow(), lockDurationSecond, false);
                logger.debug("Lock does not exist. Creating new lock" + tempLock);
                return createLock(tempLock);
            }
        } catch (VersionConflictEngineException e) {
            logger.debug("could not acquire lock {}", e.getMessage());
            return null;
        }
    }

    private boolean isLockReleasedOrExpired(final LockModel lock) {
        return lock.isReleased() || lock.isExpired();
    }

    private LockModel updateLock(final LockModel updateLock) {
        try {
            UpdateRequest updateRequest = new UpdateRequest()
                .index(LOCK_INDEX_NAME)
                .id(updateLock.getLockId())
                .setIfSeqNo(updateLock.getSeqNo())
                .setIfPrimaryTerm(updateLock.getPrimaryTerm())
                .doc(updateLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .fetchSource(true);

            final UpdateResponse updateResponse = client.update(updateRequest).actionGet();

            return new LockModel(updateLock, updateResponse.getSeqNo(), updateResponse.getPrimaryTerm());
        } catch (VersionConflictEngineException e) {
            logger.debug("could not acquire lock {}", e.getMessage());
        } catch (DocumentMissingException e) {
            logger.debug("Document is deleted. This happens if the job is already removed and this is the last run." +
                "{}", e.getMessage());
        } catch (IOException e) {
            logger.error("IOException occurred updating lock.", e);
        }
        return null;
    }

    private LockModel createLock(final LockModel tempLock) {
        try {
            final IndexRequest request = new IndexRequest(LOCK_INDEX_NAME)
                .id(tempLock.getLockId())
                .source(tempLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
                .setIfPrimaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                .create(true);
            final IndexResponse indexResponse = client.index(request).actionGet();
            return new LockModel(tempLock, indexResponse.getSeqNo(), indexResponse.getPrimaryTerm());
        } catch (VersionConflictEngineException e) {
            logger.debug("Lock is already created. {}", e.getMessage());
        } catch (IOException e) {
            logger.error("IOException occurred creating lock", e);
        }
        return null;
    }

    private LockModel findLock(final String lockId) {
        GetRequest getRequest = new GetRequest(LOCK_INDEX_NAME).id(lockId);
        GetResponse getResponse = client.get(getRequest).actionGet();
        if (!getResponse.isExists()) {
            return null;
        } else {
            try {
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString());
                parser.nextToken();
                return LockModel.parse(parser, getResponse.getSeqNo(), getResponse.getPrimaryTerm());
            } catch (IOException e) {
                logger.error("IOException occurred finding lock", e);
                return null;
            }
        }
    }

    public boolean release(final LockModel lock) {
        if (lock == null) {
            logger.info("Lock is null. Nothing to release.");
            return false;
        }

        logger.debug("Releasing lock: " + lock);
        final LockModel lockToRelease = new LockModel(lock, true);
        final LockModel releasedLock = updateLock(lockToRelease);
        return releasedLock != null;
    }

    public boolean deleteLock(final String lockId) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(LOCK_INDEX_NAME).id(lockId);
            DeleteResponse deleteResponse = client.delete(deleteRequest).actionGet();
            return deleteResponse.getResult() == DocWriteResponse.Result.DELETED ||
                deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND;
        } catch (IndexNotFoundException e){
            logger.debug("Index is not found to delete lock. {}", e.getMessage());
            // Index does not exist. There is nothing to delete.
        }
        return true;
    }

    private Instant getNow() {
        return testInstant != null ? testInstant : Instant.now();
    }

    @VisibleForTesting
    void setTime(final Instant testInstant) {
        this.testInstant = testInstant;
    }
}
