package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class GetJobNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private Map<String, Object> jobInfoMap;

    public GetJobNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.jobInfoMap = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    public GetJobNodeResponse(DiscoveryNode node, Map<String, Object> jobInfoMap) {
        super(node);
        this.jobInfoMap = jobInfoMap;
    }

    public static GetJobNodeResponse readJobInfo(StreamInput in) throws IOException {
        GetJobNodeResponse jobInfo = new GetJobNodeResponse(in);
        return jobInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(jobInfoMap, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for(String jobId: jobInfoMap.keySet()) {
            builder.field(jobId, jobInfoMap.get(jobId));
        }
        return builder;
    }
}
