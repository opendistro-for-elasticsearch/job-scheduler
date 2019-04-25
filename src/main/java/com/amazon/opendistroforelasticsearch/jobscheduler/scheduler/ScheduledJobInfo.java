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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Job index, id and jobInfo mapping.
 */
public class ScheduledJobInfo {
    private Map<String, Map<String, JobSchedulingInfo>> jobInfoMap;

    ScheduledJobInfo() {
        this.jobInfoMap = new ConcurrentHashMap<>();
    }

    public Map<String, JobSchedulingInfo> getJobsByIndex(String indexName) {
        if(!this.jobInfoMap.containsKey(indexName)) {
            synchronized (this.jobInfoMap) {
                if(!this.jobInfoMap.containsKey(indexName)) {
                    this.jobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }
        return this.jobInfoMap.get(indexName);
    }

    public JobSchedulingInfo getJobInfo(String indexName, String jobId) {
        return getJobsByIndex(indexName).get(jobId);
    }

    public void addJob(String indexName, String jobId, JobSchedulingInfo jobInfo) {
        if(!this.jobInfoMap.containsKey(indexName)) {
            synchronized (this.jobInfoMap) {
                if(!this.jobInfoMap.containsKey(indexName)) {
                    jobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }

        this.jobInfoMap.get(indexName).put(jobId, jobInfo);
    }

    public JobSchedulingInfo removeJob(String indexName, String jobId) {
        if(this.jobInfoMap.containsKey(indexName)) {
            return this.jobInfoMap.get(indexName).remove(jobId);
        }

        return null;
    }
}
