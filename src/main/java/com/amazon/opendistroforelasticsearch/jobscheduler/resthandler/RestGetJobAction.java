package com.amazon.opendistroforelasticsearch.jobscheduler.resthandler;

import com.amazon.opendistroforelasticsearch.jobscheduler.transport.action.GetJobAction;
import com.amazon.opendistroforelasticsearch.jobscheduler.transport.action.GetJobRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RestGetJobAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_job_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new Route(RestRequest.Method.GET, "/_opendistro/_jobscheduler/jobs")
        ));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetJobRequest getJobRequest = new GetJobRequest(".abc");
        return channel -> client.execute(GetJobAction.INSTANCE, getJobRequest, new RestToXContentListener<>(channel));
    }
}
