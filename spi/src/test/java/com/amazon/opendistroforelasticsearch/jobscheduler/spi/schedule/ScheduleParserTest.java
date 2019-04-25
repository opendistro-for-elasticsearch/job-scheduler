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

package com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class ScheduleParserTest {

    @Test
    public void testParseCronSchedule() throws IOException {
        String cronScheduleJsonStr = "{\"cron\":{\"expression\":\"* * * * *\",\"timezone\":\"PST8PDT\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(cronScheduleJsonStr));
        parser.nextToken();
        Schedule schedule = ScheduleParser.parse(parser);

        Assert.assertTrue(schedule instanceof CronSchedule);
        Assert.assertEquals("* * * * *", ((CronSchedule)schedule).getCronExpression());
        Assert.assertEquals(ZoneId.of("PST8PDT"), ((CronSchedule)schedule).getTimeZone());
    }

    @Test
    public void testParseIntervalSchedule() throws IOException {
        String intervalScheduleJsonStr = "{\"interval\":{\"start_time\":1546329600000,\"period\":1,\"unit\":\"Minutes\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(intervalScheduleJsonStr));
        parser.nextToken();
        Schedule schedule = ScheduleParser.parse(parser);

        Assert.assertTrue(schedule instanceof IntervalSchedule);
        Assert.assertEquals(Instant.ofEpochMilli(1546329600000L), ((IntervalSchedule)schedule).getStartTime());
        Assert.assertEquals(1, ((IntervalSchedule)schedule).getInterval());
        Assert.assertEquals(ChronoUnit.MINUTES, ((IntervalSchedule)schedule).getUnit());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testUnknownScheudleType() throws IOException {
        String scheduleJsonStr = "{\"unknown_type\":{\"field\":\"value\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(scheduleJsonStr));
        parser.nextToken();
        ScheduleParser.parse(parser);
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_unknownFieldInCronSchedule() throws IOException {
        String cronScheduleJsonStr = "{\"cron\":{\"expression\":\"* * * * *\",\"unknown_field\":\"value\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(cronScheduleJsonStr));
        parser.nextToken();
        ScheduleParser.parse(parser);
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_unknownFiledInIntervalSchedule() throws IOException {
        String intervalScheduleJsonStr = "{\"interval\":{\"start_time\":1546329600000,\"period\":1,\"unknown_filed\":\"value\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(intervalScheduleJsonStr));
        parser.nextToken();
        ScheduleParser.parse(parser);
    }

    private XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
        return xContent.createParser(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
                LoggingDeprecationHandler.INSTANCE, data.streamInput());
    }
}
