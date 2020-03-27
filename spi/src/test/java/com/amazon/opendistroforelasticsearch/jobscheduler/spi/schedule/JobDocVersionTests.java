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

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobDocVersion;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

public class JobDocVersionTests extends ESTestCase {

    public void testCompareTo() {
        // constructor parameters: primary_term, seqNo, version
        JobDocVersion version1 = new JobDocVersion(1L, 1L, 1L);
        Assert.assertTrue(version1.compareTo(null) > 0);

        JobDocVersion version2 = new JobDocVersion(1L, 2L, 1L);
        Assert.assertTrue(version1.compareTo(version2) < 0);

        JobDocVersion version3 = new JobDocVersion(2L, 1L, 1L);
        Assert.assertTrue(version1.compareTo(version3) < 0);
        Assert.assertTrue(version2.compareTo(version3) > 0);

        JobDocVersion version4 = new JobDocVersion(1L, 1L, 1L);
        Assert.assertTrue(version1.compareTo(version4) == 0);
    }
}
