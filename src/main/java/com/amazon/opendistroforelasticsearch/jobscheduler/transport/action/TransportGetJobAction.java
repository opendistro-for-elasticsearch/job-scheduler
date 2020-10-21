/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobScheduler;
import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobSchedulerMetrics;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportGetJobAction extends TransportNodesAction<GetJobRequest, GetJobResponse, GetJobNodeRequest, GetJobNodeResponse> {

    private JobScheduler scheduler;

    @Inject
    public TransportGetJobAction(ClusterService clusterService, ThreadPool threadPool, TransportService transportService,
                                 ActionFilters actionFilters, JobScheduler scheduler){
        super(GetJobAction.NAME, threadPool, clusterService, transportService, actionFilters, GetJobRequest::new, GetJobNodeRequest::new,
                ThreadPool.Names.MANAGEMENT, GetJobNodeResponse.class);
        this.scheduler = scheduler;
    }

    @Override
    protected GetJobResponse newResponse(GetJobRequest request, List<GetJobNodeResponse> responses, List<FailedNodeException> failures) {
        return new GetJobResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected GetJobNodeRequest newNodeRequest(GetJobRequest request){
        return new GetJobNodeRequest(request);
    }

    @Override
    protected GetJobNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetJobNodeResponse(in);
    }

    @Override
    protected GetJobNodeResponse nodeOperation(GetJobNodeRequest request) {
        List<JobSchedulerMetrics> jobSchedulerMetrics = this.scheduler.getJobSchedulerMetrics(request.getIndexName());
        JobSchedulerMetrics[] jobInfos = new JobSchedulerMetrics[jobSchedulerMetrics.size()];
        return new GetJobNodeResponse(clusterService.localNode(), jobSchedulerMetrics.toArray(jobInfos), request.getIndexName());
    }
}
