/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.jobscheduler.transport.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class GetJobResponse extends BaseNodesResponse<GetJobNodeResponse> implements ToXContentObject {

    public GetJobResponse(ClusterName clusterName, List<GetJobNodeResponse> nodes, List<FailedNodeException> failures){
        super(clusterName, nodes, failures);
    }

    public GetJobResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out){
    }

    @Override
    protected List<GetJobNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetJobNodeResponse::readJobInfo);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<GetJobNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (GetJobNodeResponse nodeResponse : getNodes()) {
            builder.startObject(nodeResponse.getNode().getId());
            nodeResponse.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}
