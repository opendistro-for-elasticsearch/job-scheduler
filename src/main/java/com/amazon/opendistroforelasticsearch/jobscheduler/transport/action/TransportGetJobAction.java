package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobScheduler;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

public class TransportGetJobAction extends HandledTransportAction<GetJobRequest, GetJobResponse> {

    private JobScheduler jobScheduler;

    @Inject
    public TransportGetJobAction(TransportService transportService, ActionFilters actionFilters, JobScheduler jobScheduler){
        super(GetJobAction.NAME, transportService, actionFilters, GetJobRequest::new);
        this.jobScheduler = jobScheduler;
    }

    @Override
    public void doExecute(Task task, GetJobRequest getJobRequest, ActionListener<GetJobResponse> actionListener) {
        //Set<String> ids = this.jobScheduler.getScheduledJobIds(getJobRequest.indexName);
        //String id = ids.iterator().next();
        //actionListener.onResponse(new GetJobResponse(id));
    }
}
