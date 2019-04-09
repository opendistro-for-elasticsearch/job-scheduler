package com.amazon.opendistro.jobscheduler.spi;

import java.time.Instant;

public class JobExecutionContext {
    private Instant expectedExecutionTime;

    public Instant getExpectedExecutionTime() {
        return this.expectedExecutionTime;
    }

    public void setExpectedExecutionTime(Instant expectedExecutionTime) {
        this.expectedExecutionTime = expectedExecutionTime;
    }
}
