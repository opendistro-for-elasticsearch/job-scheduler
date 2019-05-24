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

package com.amazon.opendistroforelasticsearch.jobscheduler.model.lock;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class LockModel implements ToXContentObject {
    private static final String LOCK_ID_DELIMITR = "-";
    public static final String JOB_TYPE = "job_type";
    public static final String JOB_ID = "job_id";
    public static final String LOCK_TIME = "lock_time";
    public static final String LOCK_DURATION = "lock_duration_seconds";
    public static final String RELEASED = "released";

    private final String lockId;
    private final String jobType;
    private final String jobId;
    private final Instant lockTime;
    private final long lockDurationSeconds;
    private final boolean released;
    private final long seqNo;
    private final long primaryTerm;

    /**
     * Use this constructor to copy existing lock and update the seqNo and primaryTerm.
     *
     * @param copyLock    JobSchedulerLockModel to copy from.
     * @param seqNo       sequence number from Elasticsearch document.
     * @param primaryTerm primary term from Elasticsearch document.
     */
    public LockModel(final LockModel copyLock, long seqNo, long primaryTerm) {
        this(copyLock.jobType, copyLock.jobId, copyLock.lockTime, copyLock.lockDurationSeconds,
            copyLock.released, seqNo, primaryTerm);
    }

    /**
     * Use this constructor to copy existing lock and change status of the released of the lock.
     *
     * @param copyLock JobSchedulerLockModel to copy from.
     * @param released boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock, final boolean released) {
        this(copyLock.jobType, copyLock.jobId, copyLock.lockTime, copyLock.lockDurationSeconds,
            released, copyLock.seqNo, copyLock.primaryTerm);
    }

    /**
     * Use this constructor to copy existing lock and change the duration of the lock.
     *
     * @param copyLock            JobSchedulerLockModel to copy from.
     * @param updateLockTime      new updated lock time to start the lock.
     * @param lockDurationSeconds total lock duration in seconds.
     * @param released            boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock,
                     final Instant updateLockTime, final long lockDurationSeconds, final boolean released) {
        this(copyLock.jobType, copyLock.jobId, updateLockTime, lockDurationSeconds, released, copyLock.seqNo, copyLock.primaryTerm);
    }

    public LockModel(String jobType, String jobId, Instant lockTime, long lockDurationSeconds, boolean released) {
        this(jobType, jobId, lockTime, lockDurationSeconds, released,
            SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    }

    public LockModel(String jobType, String jobId, Instant lockTime,
                     long lockDurationSeconds, boolean released, long seqNo, long primaryTerm) {
        this.lockId = jobType + LOCK_ID_DELIMITR + jobId;
        this.jobType = jobType;
        this.jobId = jobId;
        this.lockTime = lockTime;
        this.lockDurationSeconds = lockDurationSeconds;
        this.released = released;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    public static String generateLockId(String jobType, String jobId) {
        return jobType + LOCK_ID_DELIMITR + jobId;
    }

    public static LockModel parse(final XContentParser parser, long seqNo, long primaryTerm) throws IOException {
        String jobType = null;
        String jobId = null;
        Instant lockTime = null;
        Long lockDurationSecond = null;
        Boolean released = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case JOB_TYPE:
                    jobType = parser.text();
                    break;
                case JOB_ID:
                    jobId = parser.text();
                    break;
                case LOCK_TIME:
                    lockTime = Instant.ofEpochSecond(parser.longValue());
                    break;
                case LOCK_DURATION:
                    lockDurationSecond = parser.longValue();
                    break;
                case RELEASED:
                    released = parser.booleanValue();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }

        return new LockModel(
            requireNonNull(jobType, "JobType cannot be null"),
            requireNonNull(jobId, "JobId cannot be null"),
            requireNonNull(lockTime, "lockTime cannot be null"),
            requireNonNull(lockDurationSecond, "lockDurationSeconds cannot be null"),
            requireNonNull(released, "released cannot be null"),
            seqNo,
            primaryTerm
        );
    }

    @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject()
            .field(JOB_TYPE, this.jobType)
            .field(JOB_ID, this.jobId)
            .field(LOCK_TIME, this.lockTime.getEpochSecond())
            .field(LOCK_DURATION, this.lockDurationSeconds)
            .field(RELEASED, this.released)
            .endObject();
        return builder;
    }

    @Override public String toString() {
        return Strings.toString(this, false, true);
    }

    public String getLockId() {
        return lockId;
    }

    public String getJobType() {
        return jobType;
    }

    public String getJobId() {
        return jobId;
    }

    public Instant getLockTime() {
        return lockTime;
    }

    public long getLockDurationSeconds() {
        return lockDurationSeconds;
    }

    public boolean isReleased() {
        return released;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }
}
