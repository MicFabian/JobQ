package com.jobq.dashboard;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobQWebMvcConfigurer implements WebMvcConfigurer {

    private final JobQAuthInterceptor authInterceptor;

    public JobQWebMvcConfigurer(JobQAuthInterceptor authInterceptor) {
        this.authInterceptor = authInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // Enforce the interceptor on all dashboard endpoints and static resources
        registry.addInterceptor(authInterceptor)
                .addPathPatterns("/jobq/**");
    }
}
