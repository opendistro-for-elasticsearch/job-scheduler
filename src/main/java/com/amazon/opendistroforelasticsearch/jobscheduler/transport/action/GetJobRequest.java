package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetJobRequest extends BaseNodesRequest<GetJobRequest> {

    private String indexName;

    public GetJobRequest(String indexName, String... nodesIds) {
        super(nodesIds);
        this.indexName = indexName;
    }

    public GetJobRequest(StreamInput in) throws IOException {
        super(in);
        indexName = in.readString();
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
    }
}
