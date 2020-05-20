/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.jobscheduler.sampleextension;

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A sample rest handler that supports schedule and deschedule job operation
 *
 * Users need to provide "id", "index", "job_name", and "interval" parameter to schedule
 * a job. e.g.
 * {@code POST /_opendistro/scheduler_sample/watch?id=kibana-job-id&job_name=watch kibana index&index=.kibana_1&interval=1"}
 *
 * creates a job with id "1" and job name "watch kibana index", which logs ".kibana_1" index's shards info
 * every 1 minute
 *
 * Users can remove that job by calling
 * {@code DELETE /_opendistro/scheduler_sample/watch?id=kibana-job-id}
 */
public class SampleExtensionRestHandler extends BaseRestHandler {
    public static final String WATCH_INDEX_URI = "/_opendistro/scheduler_sample/watch";

    @Override
    public String getName() {
        return "Sample JobScheduler extension handler";
    }

    @Override
    public List<Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new Route(RestRequest.Method.POST, WATCH_INDEX_URI),
                new Route(RestRequest.Method.DELETE, WATCH_INDEX_URI)
        ));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method().equals(RestRequest.Method.POST)) {
            // compose SampleJobParameter object from request
            String id = request.param("id");
            String indexName = request.param("index");
            String jobName = request.param("job_name");
            String interval = request.param("interval");
            String lockDurationSecondsString = request.param("lock_duration_seconds");
            Long lockDurationSeconds = lockDurationSecondsString != null ? Long.parseLong(lockDurationSecondsString) : null;
            String jitterString = request.param("jitter");
            Double jitter = jitterString != null ? Double.parseDouble(jitterString) : null;

            if(id == null || indexName ==null) {
                throw new IllegalArgumentException("Must specify id and index parameter");
            }
            SampleJobParameter jobParameter = new SampleJobParameter(id, jobName, indexName,
                    new IntervalSchedule(Instant.now(), Integer.parseInt(interval), ChronoUnit.MINUTES), lockDurationSeconds, jitter);
            IndexRequest indexRequest = new IndexRequest()
                    .index(SampleExtensionPlugin.JOB_INDEX_NAME)
                    .id(id)
                    .source(jobParameter.toXContent(JsonXContent.contentBuilder(), null));

            return restChannel -> {
                // index the job parameter
                client.index(indexRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        try {
                            RestResponse restResponse = new BytesRestResponse(RestStatus.OK,
                                    indexResponse.toXContent(JsonXContent.contentBuilder(), null));
                            restChannel.sendResponse(restResponse);
                        } catch(IOException e) {
                            restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                    }
                });
            };
        } else if (request.method().equals(RestRequest.Method.DELETE)) {
            // delete job parameter doc from index
            String id = request.param("id");
            DeleteRequest deleteRequest = new DeleteRequest()
                    .index(SampleExtensionPlugin.JOB_INDEX_NAME)
                    .id(id);

            return restChannel -> {
                client.delete(deleteRequest, new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, "Job deleted."));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                    }
                });
            };
        } else {
            return restChannel -> {
                restChannel.sendResponse(new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, request.method() + " is not allowed."));
            };
        }
    }
}
