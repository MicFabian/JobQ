package com.jobq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
@ConditionalOnProperty(prefix = "jobq.schema", name = "auto-initialize", havingValue = "true", matchIfMissing = true)
public class JobSchemaInitializer implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(JobSchemaInitializer.class);
    private final DataSource dataSource;

    public JobSchemaInitializer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void afterPropertiesSet() {
        log.info("Initializing JobQ database schema...");
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new ClassPathResource("jobq-schema.sql"));
            populator.execute(dataSource);
            log.info("JobQ database schema initialized successfully.");
        } catch (Exception e) {
            // Already handled if table exist, but usually ignored gracefully
            log.error("Failed to initialize JobQ database schema.", e);
        }
    }
}
