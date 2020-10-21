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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class GetJobRequestTests extends ESTestCase {

    public void testGetJobRequest() throws IOException {
        String[] jobIndexNames = {".job_index_1", ".job_index_2"};
        GetJobRequest request = new GetJobRequest(jobIndexNames, "nodeId");
        assertNotNull(request);

        // validate writeTo()
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        GetJobRequest newRequest = new GetJobRequest(in);
        assertArrayEquals("Expected jobIndexNames to be equal", jobIndexNames, newRequest.getJobIndexNames());
    }
}
