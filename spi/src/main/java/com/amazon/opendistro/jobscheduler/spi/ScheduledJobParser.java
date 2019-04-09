package com.amazon.opendistro.jobscheduler.spi;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public interface ScheduledJobParser {
    ScheduledJobParameter parse(XContentParser xContentParser, String id, Long version) throws IOException;
}
