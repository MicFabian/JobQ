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
            assertTrue(properties.getDashboard().isEnabled());
            assertEquals("/jobq/dashboard", properties.getDashboard().getPath());
            assertFalse(properties.getDatabase().isSkipCreate());
        });
    }

    @Test
    void shouldMapCustomJobqProperties() {
        contextRunner
                .withPropertyValues(
                        "jobq.background-job-server.poll-interval-in-seconds=1",
                        "jobq.background-job-server.worker-count=5",
                        "jobq.dashboard.enabled=false",
                        "jobq.dashboard.path=/custom/path",
                        "jobq.database.skip-create=true")
                .run(context -> {
                    JobQProperties properties = context.getBean(JobQProperties.class);
                    assertEquals(1, properties.getBackgroundJobServer().getPollIntervalInSeconds());
                    assertEquals(5, properties.getBackgroundJobServer().getWorkerCount());
                    assertFalse(properties.getDashboard().isEnabled());
                    assertEquals("/custom/path", properties.getDashboard().getPath());
                    assertTrue(properties.getDatabase().isSkipCreate());
                });
    }
}
