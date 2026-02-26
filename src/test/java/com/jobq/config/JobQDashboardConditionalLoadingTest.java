package com.jobq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.JobRepository;
import com.jobq.dashboard.JobQAuthInterceptor;
import com.jobq.dashboard.JobQDashboardController;
import com.jobq.dashboard.JobQWebMvcConfigurer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JobQDashboardConditionalLoadingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(JobQDashboardController.class, JobQMvcConfiguration.class, JobQWebMvcConfigurer.class)
            .withBean(JobQProperties.class, JobQProperties::new)
            .withBean(JobRepository.class, () -> mock(JobRepository.class))
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withBean(JobQAuthInterceptor.class, () -> new JobQAuthInterceptor(new JobQProperties()));

    @Test
    void shouldNotLoadDashboardBeansByDefault() {
        contextRunner.run(context -> {
            assertTrue(context.getBeansOfType(JobQDashboardController.class).isEmpty());
            assertTrue(context.getBeansOfType(JobQMvcConfiguration.class).isEmpty());
            assertTrue(context.getBeansOfType(JobQWebMvcConfigurer.class).isEmpty());
        });
    }

    @Test
    void shouldLoadDashboardBeansWhenEnabledExplicitly() {
        contextRunner
                .withPropertyValues("jobq.dashboard.enabled=true")
                .run(context -> {
                    assertFalse(context.getBeansOfType(JobQDashboardController.class).isEmpty());
                    assertFalse(context.getBeansOfType(JobQMvcConfiguration.class).isEmpty());
                    assertFalse(context.getBeansOfType(JobQWebMvcConfigurer.class).isEmpty());
                });
    }
}
