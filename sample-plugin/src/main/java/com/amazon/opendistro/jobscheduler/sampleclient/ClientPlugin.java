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

package com.amazon.opendistro.jobscheduler.sampleclient;

import com.amazon.opendistro.jobscheduler.spi.JobSchedulerExtension;
import com.amazon.opendistro.jobscheduler.spi.ScheduledJobParser;
import com.amazon.opendistro.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistro.jobscheduler.spi.schedule.ScheduleParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.time.Instant;

public class ClientPlugin extends Plugin implements JobSchedulerExtension {
    private static final Logger log = LogManager.getLogger(ClientPlugin.class);

    @Override
    public String getJobType() {
        return "sample_client";
    }

    @Override
    public String getJobIndex() {
        return ".sample_client_index";
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return (job, context) -> {
            log.info("SampleClient runner invoked.");
        };
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (parser, id, version) -> {
            SampleJobParameter jobParameter = new SampleJobParameter();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

            while(!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
                String filedName = parser.currentName();
                parser.nextToken();
                switch (filedName) {
                    case SampleJobParameter.NAME_FIELD:
                        jobParameter.setName(parser.text());
                        break;
                    // case SampleJobParameter.ID_FIELD: jobParameter.setId(parser.text()); break;
                    case SampleJobParameter.ENABLED_FILED:
                        jobParameter.setEnabled(parser.booleanValue());
                        break;
                    case SampleJobParameter.ENABLE_TIME_FILED:
                        jobParameter.setEnableTime(parseInstantValue(parser));
                        break;
                    case SampleJobParameter.LAST_UPDATE_TIME_FIELD:
                        jobParameter.setLastUpdateTime(parseInstantValue(parser));
                        break;
                    case SampleJobParameter.SCHEDULE_FIELD:
                        jobParameter.setSchedule(ScheduleParser.parse(parser));
                        break;
                    case SampleJobParameter.SAMPLE_PARAM_FILED:
                        jobParameter.setSampleParam(parser.text());
                        break;
                    default:
                        XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
                }
            }

            return jobParameter;
        };
    }

    private Instant parseInstantValue(XContentParser parser) throws IOException {
        if(XContentParser.Token.VALUE_NULL.equals(parser.currentToken())) {
            return null;
        }
        if(parser.currentToken().isValue()) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
        return null;
    }
}
