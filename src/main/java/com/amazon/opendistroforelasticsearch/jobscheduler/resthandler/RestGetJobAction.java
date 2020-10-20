package com.amazon.opendistroforelasticsearch.jobscheduler.resthandler;

import com.amazon.opendistroforelasticsearch.jobscheduler.JobSchedulerPlugin;
import com.amazon.opendistroforelasticsearch.jobscheduler.transport.action.GetJobAction;
import com.amazon.opendistroforelasticsearch.jobscheduler.transport.action.GetJobRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RestGetJobAction extends BaseRestHandler {

    private Set<String> indicesToListen;

    public RestGetJobAction(Set<String> indicesToListen) {
        this.indicesToListen = indicesToListen;
    }

    @Override
    public String getName() {
        return "get_job_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new Route(RestRequest.Method.GET,JobSchedulerPlugin.JOB_SCHEDULER_BASE_URI + "/{nodeId}/jobs"),
                new Route(RestRequest.Method.GET, JobSchedulerPlugin.JOB_SCHEDULER_BASE_URI + "/jobs")
        ));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        GetJobRequest getJobRequest = new GetJobRequest(indicesToListen.iterator().next(), nodesIds);
        return channel -> client.execute(GetJobAction.INSTANCE, getJobRequest, new RestActions.NodesResponseRestListener<>(channel));
    }
}
