package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jobq.config.JobQProperties;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.naming.PhysicalNamingStrategySnakeCaseImpl;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Locale;

@AutoConfiguration
@ComponentScan("com.jobq")
@EntityScan(basePackageClasses = Job.class)
@EnableJpaRepositories(basePackageClasses = JobRepository.class)
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
                        new PhysicalNamingStrategySnakeCaseImpl() {
                            @Override
                            public Identifier toPhysicalTableName(Identifier name,
                                    org.hibernate.engine.jdbc.env.spi.JdbcEnvironment jdbcEnvironment) {
                                Identifier original = super.toPhysicalTableName(name, jdbcEnvironment);
                                // Only intercept JobQ tables
                                if (original.getText().toLowerCase(Locale.ROOT).startsWith("jobq_")) {
                                    return new Identifier(prefix + original.getText(), original.isQuoted());
                                }
                                return original;
                            }
                        });
            }
        };
    }

}
