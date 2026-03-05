package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jobq.config.JobQProperties;
import jakarta.persistence.EntityManager;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.naming.PhysicalNamingStrategySnakeCaseImpl;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactoryBean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Locale;
import java.util.UUID;

@AutoConfiguration
@AutoConfigurationPackage
@Import(JobQEntityScanRegistrar.class)
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

    @Bean
    @ConditionalOnMissingBean(JobRepository.class)
    public JpaRepositoryFactoryBean<JobRepository, Job, UUID> jobqRepositoryFactoryBean(EntityManager entityManager) {
        JpaRepositoryFactoryBean<JobRepository, Job, UUID> factoryBean = new JpaRepositoryFactoryBean<>(
                JobRepository.class);
        factoryBean.setEntityManager(entityManager);
        return factoryBean;
    }

}
