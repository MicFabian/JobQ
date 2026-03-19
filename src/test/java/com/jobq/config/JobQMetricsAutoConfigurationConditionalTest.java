package com.jobq.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.jobq.JobQMetricsAutoConfiguration;
import com.jobq.JobRepository;
import com.jobq.internal.JobQMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class JobQMetricsAutoConfigurationConditionalTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(JobQMetricsAutoConfiguration.class));

    @Test
    void shouldNotCreateMetricsBeanWithoutMeterRegistryBean() {
        contextRunner
                .withBean(JobRepository.class, () -> mock(JobRepository.class))
                .run(context ->
                        assertTrue(context.getBeansOfType(JobQMetrics.class).isEmpty()));
    }

    @Test
    void shouldBackOffWhenMicrometerIsNotOnClasspath() {
        contextRunner
                .withClassLoader(new FilteredClassLoader("io.micrometer.core.instrument"))
                .withBean(JobRepository.class, () -> mock(JobRepository.class))
                .run(context -> {
                    assertTrue(context.getBeansOfType(JobQMetrics.class).isEmpty());
                    assertFalse(context.containsBean("jobqMetrics"));
                });
    }
}
