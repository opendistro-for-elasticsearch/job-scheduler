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
    String indexName;

    public GetJobNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.jobInfos = in.readOptionalArray(JobSchedulerMetrics::new, JobSchedulerMetrics[]::new);
        this.indexName = in.readString();
    }

    public GetJobNodeResponse(DiscoveryNode node, JobSchedulerMetrics[] jobInfos, String indexName) {
        super(node);
        this.jobInfos = jobInfos;
        this.indexName = indexName;
    }

    public static GetJobNodeResponse readJobInfo(StreamInput in) throws IOException {
        return new GetJobNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalArray(jobInfos);
        out.writeString(indexName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (jobInfos != null) {
            builder.startObject(indexName);
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
