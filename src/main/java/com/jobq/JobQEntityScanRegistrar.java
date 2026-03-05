package com.jobq;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.persistence.autoconfigure.EntityScanPackages;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

class JobQEntityScanRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        EntityScanPackages.register(registry, Job.class.getPackageName());
    }
}
