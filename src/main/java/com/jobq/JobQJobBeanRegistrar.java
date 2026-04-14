package com.jobq;

import com.jobq.annotation.Job;
import java.util.List;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;

@Component
class JobQJobBeanRegistrar
        implements BeanDefinitionRegistryPostProcessor,
                BeanFactoryAware,
                EnvironmentAware,
                ResourceLoaderAware,
                PriorityOrdered {

    private final BeanNameGenerator beanNameGenerator = AnnotationBeanNameGenerator.INSTANCE;

    private ConfigurableListableBeanFactory beanFactory;
    private Environment environment;
    private ResourceLoader resourceLoader;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ConfigurableListableBeanFactory configurableListableBeanFactory) {
            this.beanFactory = configurableListableBeanFactory;
        }
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        if (beanFactory == null || !AutoConfigurationPackages.has(beanFactory)) {
            return;
        }

        List<String> basePackages = AutoConfigurationPackages.get(beanFactory);
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false, environment);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Job.class));
        if (resourceLoader != null) {
            scanner.setResourceLoader(resourceLoader);
        }

        for (String basePackage : basePackages) {
            for (BeanDefinition candidate : scanner.findCandidateComponents(basePackage)) {
                String className = candidate.getBeanClassName();
                if (!StringUtils.hasText(className) || className.contains("$")) {
                    continue;
                }
                if (hasExistingBeanDefinitionForClass(registry, className)) {
                    continue;
                }

                String beanName = beanNameGenerator.generateBeanName(candidate, registry);
                if (registry.containsBeanDefinition(beanName)) {
                    continue;
                }
                registry.registerBeanDefinition(beanName, candidate);
            }
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {}

    private boolean hasExistingBeanDefinitionForClass(BeanDefinitionRegistry registry, String className) {
        for (String beanName : registry.getBeanDefinitionNames()) {
            BeanDefinition existing = registry.getBeanDefinition(beanName);
            if (className.equals(existing.getBeanClassName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
