package com.jobq.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobQMvcConfiguration implements WebMvcConfigurer {

    private final JobQProperties properties;

    public JobQMvcConfiguration(JobQProperties properties) {
        this.properties = properties;
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // Serve static assets out of the classpath jobq directory.
        registry.addResourceHandler("/jobq/assets/**")
                .addResourceLocations("classpath:/static/jobq/");
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        // Map the configurable dashboard path to the JobQ index.html
        String path = properties.getDashboard().getPath();
        registry.addViewController(path).setViewName("forward:/jobq/assets/index.html");
    }
}
