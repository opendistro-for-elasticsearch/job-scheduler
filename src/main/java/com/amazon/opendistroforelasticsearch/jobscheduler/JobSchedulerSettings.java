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

package com.amazon.opendistroforelasticsearch.jobscheduler;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

public class JobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "opendistro.jobscheduler.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "opendistro.jobscheduler.sweeper.backoff_millis",
            TimeValue.timeValueMillis(50),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "opendistro.jobscheduler.retry_count",
            3,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
            "opendistro.jobscheduler.sweeper.period",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
            "opendistro.jobscheduler.sweeper.page_size",
            100,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
}
