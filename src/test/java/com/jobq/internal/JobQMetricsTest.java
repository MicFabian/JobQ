package com.jobq.internal;

import com.jobq.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JobQMetricsTest {

    private JobRepository jobRepository;
    private MeterRegistry meterRegistry;
    private JobQMetrics jobQMetrics;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        meterRegistry = new SimpleMeterRegistry();
        jobQMetrics = new JobQMetrics(jobRepository, meterRegistry);
    }

    @Test
    void shouldRegisterGaugesForJobStatuses() {
        when(jobRepository.countPendingJobs()).thenReturn(10L);
        when(jobRepository.countProcessingJobs()).thenReturn(5L);
        when(jobRepository.countCompletedJobs()).thenReturn(100L);
        when(jobRepository.countFailedJobs()).thenReturn(2L);
        when(jobRepository.count()).thenReturn(117L);

        jobQMetrics.registerMetrics();

        Gauge pendingGauge = meterRegistry.find("jobq.jobs.count").tag("status", "PENDING").gauge();
        assertThat(pendingGauge).isNotNull();
        assertThat(pendingGauge.value()).isEqualTo(10.0);

        Gauge processingGauge = meterRegistry.find("jobq.jobs.count").tag("status", "PROCESSING").gauge();
        assertThat(processingGauge).isNotNull();
        assertThat(processingGauge.value()).isEqualTo(5.0);

        Gauge totalGauge = meterRegistry.find("jobq.jobs.total").gauge();
        assertThat(totalGauge).isNotNull();
        assertThat(totalGauge.value()).isEqualTo(117.0);
    }
}
