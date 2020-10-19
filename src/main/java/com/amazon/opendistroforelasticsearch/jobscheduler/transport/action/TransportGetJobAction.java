package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportGetJobAction extends TransportNodesAction<GetJobRequest, GetJobResponse, GetJobNodeRequest, GetJobNodeResponse> {

    private Logger log = LogManager.getLogger(TransportGetJobAction.class);
    private JobScheduler scheduler;

    @Inject
    public TransportGetJobAction(ClusterService clusterService, ThreadPool threadPool, TransportService transportService,
                                 ActionFilters actionFilters, JobScheduler scheduler){
        super(GetJobAction.NAME, threadPool, clusterService, transportService, actionFilters, GetJobRequest::new, GetJobNodeRequest::new,
                ThreadPool.Names.MANAGEMENT, GetJobNodeResponse.class);
        this.scheduler = scheduler;
    }

//    @Override
//    protected void doExecute(Task task, GetJobRequest getJobRequest, ActionListener<GetJobResponse> actionListener) {
//        Set<String> jobIdSet = this.scheduler.getScheduledJobIds(getJobRequest.indexName);
//        for (String jobId: jobIdSet) {
//            this.scheduler.getJobInfo(getJobRequest.indexName, jobId);
//        }
//        actionListener.onResponse(new GetJobResponse());
//    }

    @Override
    protected GetJobResponse newResponse(GetJobRequest request, List<GetJobNodeResponse> responses, List<FailedNodeException> failures) {
        log.info("new response");
        return new GetJobResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected GetJobNodeRequest newNodeRequest(GetJobRequest request){
        log.info("new node request");
        return new GetJobNodeRequest(request);
    }

    @Override
    protected GetJobNodeResponse newNodeResponse(StreamInput in) throws IOException {
        log.info("new node response");
        return new GetJobNodeResponse(in);
    }

    @Override
    protected GetJobNodeResponse nodeOperation(GetJobNodeRequest request) {
        log.info("node operation");
        Map<String, Object> jobInfoMap = new HashMap<>();
        log.info("index is:" + request.indexName);
        Set<String> jobIdSet = this.scheduler.getScheduledJobIds(request.indexName);
        for (String jobId: jobIdSet) {
            log.info("jobId is:" + jobId);
            // to do: compose the map
            jobInfoMap.put(jobId, this.scheduler.getJobInfo(request.indexName, jobId).getExpectedExecutionTime().getEpochSecond());
        }
        return new GetJobNodeResponse(clusterService.localNode(), jobInfoMap);
    }
}
