package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GetJobResponse extends ActionResponse implements ToXContentObject {

    String id;

    public GetJobResponse(String id){
        super();
        this.id = id;
    }


    public GetJobResponse(StreamInput in) throws IOException {
        super(in);
        in.readString(); // id
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .startObject(id)
                .field("testing response", "ID is: $id")
                .endObject()
                .endObject();
    }
}
