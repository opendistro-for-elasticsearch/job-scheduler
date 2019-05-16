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

package com.amazon.opendistroforelasticsearch.jobscheduler.spi;

/**
 * Structure to represent scheduled job document version. JobScheduler use this to determine this job
 */
public class JobDocVersion implements Comparable<JobDocVersion> {
    private final long primaryTerm;
    private final long seqNo;
    private final long version;

    public JobDocVersion(long primaryTerm, long seqNo, long version) {
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.version = version;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getVersion() {
        return version;
    }

    /**
     * Compare two doc versions. Refer to https://github.com/elastic/elasticsearch/issues/10708
     *
     * @param v the doc version to compare.
     * @return -1 if this < v, 0 if this == v, otherwise 1;
     */
    @Override
    public int compareTo(JobDocVersion v) {
        if (v == null) {
            return 1;
        }
        if (this.seqNo < v.seqNo) {
            return -1;
        }
        if (this.seqNo > v.seqNo) {
            return 1;
        }
        if(this.primaryTerm < v.primaryTerm) {
            return -1;
        }
        if(this.primaryTerm > v.primaryTerm) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return String.format("{_version: %s, _primary_term: %s, _seq_no: %s}", this.version, this.primaryTerm, this.seqNo);
    }
}
