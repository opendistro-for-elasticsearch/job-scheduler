package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetJobNodeRequest extends BaseNodeRequest {

    private GetJobRequest request;
    private String indexName;

    public GetJobNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new GetJobRequest(in);
        indexName = in.readString();
    }

    public GetJobNodeRequest(GetJobRequest request) {
        super();
        this.request = request;
        this.indexName = request.getIndexName();
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
        out.writeString(indexName);
    }
}
