package com.jobq.internal;

import com.jobq.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        JobRepository.LifecycleCounts counts = mock(JobRepository.LifecycleCounts.class);
        when(counts.getPendingCount()).thenReturn(10L);
        when(counts.getProcessingCount()).thenReturn(5L);
        when(counts.getCompletedCount()).thenReturn(100L);
        when(counts.getFailedCount()).thenReturn(2L);
        when(jobRepository.countLifecycleCounts()).thenReturn(counts);

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

        verify(jobRepository, times(1)).countLifecycleCounts();
    }
}
