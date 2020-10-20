package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import com.amazon.opendistroforelasticsearch.jobscheduler.scheduler.JobSchedulerMetrics;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GetJobNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private JobSchedulerMetrics[] jobInfos;

    public GetJobNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.jobInfos = in.readOptionalArray(JobSchedulerMetrics::new, JobSchedulerMetrics[]::new);
    }

    public GetJobNodeResponse(DiscoveryNode node, JobSchedulerMetrics[] jobInfos) {
        super(node);
        this.jobInfos = jobInfos;
    }

    public static GetJobNodeResponse readJobInfo(StreamInput in) throws IOException {
        return new GetJobNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalArray(jobInfos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (jobInfos != null) {
            builder.startObject("jobs_info");
            for (JobSchedulerMetrics job : jobInfos) {
                builder.startObject(job.getScheduledJobId());
                job.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }
}
