package com.jobq;

import com.jobq.internal.JobQMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = JobQAutoConfiguration.class)
@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
public class JobQMetricsAutoConfiguration {

    @Bean
    @ConditionalOnBean({ JobRepository.class, MeterRegistry.class })
    @ConditionalOnMissingBean
    public JobQMetrics jobqMetrics(JobRepository jobRepository, MeterRegistry meterRegistry) {
        return new JobQMetrics(jobRepository, meterRegistry);
    }
}
