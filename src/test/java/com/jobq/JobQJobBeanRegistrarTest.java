package com.jobq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jobq.registrar.sample.RegistrarScannedJob;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.support.GenericApplicationContext;

class JobQJobBeanRegistrarTest {

    @Test
    void shouldRegisterAnnotatedJobsFromAutoConfigurationPackages() {
        GenericApplicationContext context = new GenericApplicationContext();
        AutoConfigurationPackages.register(context, "com.jobq.registrar.sample");

        JobQJobBeanRegistrar registrar = newRegistrar(context);
        registrar.postProcessBeanDefinitionRegistry(context);
        context.refresh();

        try {
            assertTrue(context.containsBeanDefinition("registrarScannedJob"));
            Object bean = context.getBean("registrarScannedJob");
            assertNotNull(bean);
            assertEquals(RegistrarScannedJob.class, bean.getClass());
        } finally {
            context.close();
        }
    }

    @Test
    void shouldNotRegisterSameAnnotatedJobTwiceWhenAlreadyPresent() {
        GenericApplicationContext context = new GenericApplicationContext();
        AutoConfigurationPackages.register(context, "com.jobq.registrar.sample");
        context.registerBeanDefinition(
                "existingRegistrarScannedJob", new RootBeanDefinition(RegistrarScannedJob.class));

        JobQJobBeanRegistrar registrar = newRegistrar(context);
        registrar.postProcessBeanDefinitionRegistry(context);

        long matchingDefinitions = java.util.Arrays.stream(context.getBeanDefinitionNames())
                .map(context::getBeanDefinition)
                .filter(definition -> RegistrarScannedJob.class.getName().equals(definition.getBeanClassName()))
                .count();

        context.close();
        assertEquals(1L, matchingDefinitions);
    }

    @Test
    void shouldDoNothingWhenNoAutoConfigurationPackagesAreRegistered() {
        GenericApplicationContext context = new GenericApplicationContext();

        JobQJobBeanRegistrar registrar = newRegistrar(context);
        registrar.postProcessBeanDefinitionRegistry(context);

        long matchingDefinitions = java.util.Arrays.stream(context.getBeanDefinitionNames())
                .map(context::getBeanDefinition)
                .filter(definition -> RegistrarScannedJob.class.getName().equals(definition.getBeanClassName()))
                .count();

        context.close();
        assertEquals(0L, matchingDefinitions);
    }

    private JobQJobBeanRegistrar newRegistrar(GenericApplicationContext context) {
        JobQJobBeanRegistrar registrar = new JobQJobBeanRegistrar();
        registrar.setBeanFactory(context.getBeanFactory());
        registrar.setEnvironment(context.getEnvironment());
        registrar.setResourceLoader(context);
        return registrar;
    }
}
