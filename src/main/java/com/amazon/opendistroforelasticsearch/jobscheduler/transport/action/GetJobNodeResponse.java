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
import java.util.List;
import java.util.Map;

public class GetJobNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private Map<String, List<JobSchedulerMetrics>> jobInfoMap;

    public GetJobNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.jobInfoMap = in.readMapOfLists(StreamInput::readString, JobSchedulerMetrics::new);
    }

    public GetJobNodeResponse(DiscoveryNode node, Map<String, List<JobSchedulerMetrics>> jobInfoMap) {
        super(node);
        this.jobInfoMap = jobInfoMap;
    }

    public static GetJobNodeResponse readJobInfo(StreamInput in) throws IOException {
        return new GetJobNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMapOfLists(jobInfoMap, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!jobInfoMap.isEmpty()) {
            for(Map.Entry<String, List<JobSchedulerMetrics>> entry : jobInfoMap.entrySet()) {
                builder.startObject(entry.getKey());
                for (JobSchedulerMetrics job : entry.getValue()) {
                    builder.startObject(job.getScheduledJobId());
                    job.toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        return builder;
    }
}
