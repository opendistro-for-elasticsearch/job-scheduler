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

package com.amazon.opendistro.jobscheduler.spi.schedule;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.time.Duration;
import java.time.Instant;

public interface Schedule extends ToXContentObject {

    CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));

    Instant getNextExecutionTime(Instant time);

    Duration nextTimeToExecute();

    Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime);

    Boolean runningOnTime(Instant lastExecutionTime);
}
