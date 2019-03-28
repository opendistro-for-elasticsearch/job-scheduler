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

package com.amazon.opendistro.jobscheduler;

import com.amazon.opendistro.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistro.jobscheduler.spi.ScheduledJobRunner;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ScheduledJobProvider {
    private String jobType;
    private String jobIndexName;
    private CheckedFunction<XContentParser, ScheduledJobParameter, IOException> jobParser;
    private ScheduledJobRunner jobRunner;

    public String getJobType() {
        return jobType;
    }

    public String getJobIndexName() {
        return jobIndexName;
    }

    public CheckedFunction<XContentParser, ScheduledJobParameter, IOException> getJobParser() {
        return jobParser;
    }

    public ScheduledJobRunner getJobRunner() {
        return jobRunner;
    }

    public ScheduledJobProvider(String jobType, String jobIndexName,
                                CheckedFunction<XContentParser, ScheduledJobParameter, IOException> jobParser,
                                ScheduledJobRunner jobRunner) {
        this.jobType = jobType;
        this.jobIndexName = jobIndexName;
        this.jobParser = jobParser;
        this.jobRunner = jobRunner;
    }

}
