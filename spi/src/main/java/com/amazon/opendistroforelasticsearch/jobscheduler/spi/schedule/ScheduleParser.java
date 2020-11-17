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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Schedule XContent parser.
 */
public class ScheduleParser {
    public static Schedule parse(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        while(!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case CronSchedule.CRON_FIELD:
                    String expression = null;
                    ZoneId timezone = null;
                    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
                        String cronField = parser.currentName();
                        parser.nextToken();
                        switch (cronField) {
                            case CronSchedule.EXPRESSION_FIELD: expression = parser.text();
                                break;
                            case CronSchedule.TIMEZONE_FIELD: timezone = ZoneId.of(parser.text());
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        String.format(Locale.ROOT, "Unknown cron field %s", cronField));
                        }
                    }
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(),
                            parser);
                    parser.nextToken();
                    return new CronSchedule(expression, timezone);
                case IntervalSchedule.INTERVAL_FIELD:
                    Instant startTime = null;
                    int period = 0;
                    ChronoUnit unit = null;
                    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
                        String intervalField = parser.currentName();
                        parser.nextToken();
                        switch (intervalField) {
                            case IntervalSchedule.START_TIME_FIELD:
                                startTime = Instant.ofEpochMilli(parser.longValue());
                                break;
                            case IntervalSchedule.PERIOD_FIELD:
                                period = parser.intValue();
                                break;
                            case IntervalSchedule.UNIT_FIELD:
                                unit = ChronoUnit.valueOf(parser.text().toUpperCase(Locale.ROOT));
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        String.format(Locale.ROOT, "Unknown interval field %s", intervalField));
                        }
                    }
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(),
                            parser);
                    parser.nextToken();
                    return new IntervalSchedule(startTime, period, unit);
                default:
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "Unknown schedule type %s", fieldName));
            }
        }
        throw new IllegalArgumentException("Invalid schedule document object.");
    }
}
