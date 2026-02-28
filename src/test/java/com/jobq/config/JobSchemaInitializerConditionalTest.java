package com.jobq.config;

import com.jobq.JobSchemaInitializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JobSchemaInitializerConditionalTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues("jobq.database.fail-on-migration-error=false")
            .withUserConfiguration(JobSchemaInitializer.class)
            .withBean(DataSource.class, () -> mock(DataSource.class));

    @Test
    void shouldCreateSchemaInitializerByDefault() {
        contextRunner.run(context -> assertFalse(context.getBeansOfType(JobSchemaInitializer.class).isEmpty()));
    }

    @Test
    void shouldSkipSchemaInitializerWhenConfigured() {
        contextRunner.withPropertyValues("jobq.database.skip-create=true")
                .run(context -> assertTrue(context.getBeansOfType(JobSchemaInitializer.class).isEmpty()));
    }
}
