package com.jobq.config;

import com.jobq.JobQAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.*;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

public class JobQPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(JobQProperties.class)
    static class Config {
    }

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(Config.class);

    @Test
    void shouldMapDefaultJobqProperties() {
        contextRunner.run(context -> {
            JobQProperties properties = context.getBean(JobQProperties.class);
            assertEquals(15, properties.getBackgroundJobServer().getPollIntervalInSeconds());
            assertTrue(properties.getBackgroundJobServer().getWorkerCount() >= 1);
            assertFalse(properties.getDashboard().isEnabled());
            assertEquals("/jobq/dashboard", properties.getDashboard().getPath());
            assertEquals(JobQProperties.Dashboard.AuthMode.BASIC, properties.getDashboard().getAuthMode());
            assertEquals("JOBQ_DASHBOARD", properties.getDashboard().getRequiredRole());
            assertFalse(properties.getDatabase().isSkipCreate());
        });
    }

    @Test
    void shouldMapCustomJobqProperties() {
        contextRunner
                .withPropertyValues(
                        "jobq.background-job-server.poll-interval-in-seconds=1",
                        "jobq.background-job-server.worker-count=5",
                        "jobq.dashboard.enabled=true",
                        "jobq.dashboard.path=/custom/path",
                        "jobq.dashboard.auth-mode=SPRING_SECURITY",
                        "jobq.dashboard.required-role=OPS_DASHBOARD",
                        "jobq.database.skip-create=true")
                .run(context -> {
                    JobQProperties properties = context.getBean(JobQProperties.class);
                    assertEquals(1, properties.getBackgroundJobServer().getPollIntervalInSeconds());
                    assertEquals(5, properties.getBackgroundJobServer().getWorkerCount());
                    assertTrue(properties.getDashboard().isEnabled());
                    assertEquals("/custom/path", properties.getDashboard().getPath());
                    assertEquals(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY, properties.getDashboard().getAuthMode());
                    assertEquals("OPS_DASHBOARD", properties.getDashboard().getRequiredRole());
                    assertTrue(properties.getDatabase().isSkipCreate());
                });
    }
}
