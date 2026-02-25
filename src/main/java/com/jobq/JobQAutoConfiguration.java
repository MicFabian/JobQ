package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jobq.config.JobQProperties;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import io.micrometer.core.instrument.MeterRegistry;
import com.jobq.internal.JobQMetrics;

@AutoConfiguration
@ComponentScan("com.jobq")
@EnableScheduling
@EnableConfigurationProperties(JobQProperties.class)
public class JobQAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "jobqObjectMapper")
    public ObjectMapper jobqObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    @ConditionalOnMissingBean(name = "jobqHibernatePropertiesCustomizer")
    public HibernatePropertiesCustomizer jobqHibernatePropertiesCustomizer(JobQProperties properties) {
        return hibernateProperties -> {
            String prefix = properties.getDatabase().getTablePrefix();
            if (prefix != null && !prefix.trim().isEmpty()) {
                hibernateProperties.put("hibernate.physical_naming_strategy",
                        new CamelCaseToUnderscoresNamingStrategy() {
                            @Override
                            public Identifier toPhysicalTableName(Identifier name, JdbcEnvironment jdbcEnvironment) {
                                Identifier original = super.toPhysicalTableName(name, jdbcEnvironment);
                                // Only intercept JobQ tables
                                if (original.getText().toLowerCase().startsWith("jobq_")) {
                                    return new Identifier(prefix + original.getText(), original.isQuoted());
                                }
                                return original;
                            }
                        });
            }
        };
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    public JobQMetrics jobqMetrics(JobRepository jobRepository, MeterRegistry meterRegistry) {
        return new JobQMetrics(jobRepository, meterRegistry);
    }
}
