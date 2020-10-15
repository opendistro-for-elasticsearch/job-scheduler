package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetJobRequest extends ActionRequest {

    String indexName;

    public GetJobRequest(String indexName) {
        super();
        this.indexName = indexName;
    }

    public GetJobRequest(StreamInput in) throws IOException {
        super(in);
        indexName = in.readString();
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
