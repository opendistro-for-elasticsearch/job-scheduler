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
