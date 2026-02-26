package com.jobq.dashboard;

import com.jobq.config.JobQProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true")
public class JobQWebMvcConfigurer implements WebMvcConfigurer {

    private final JobQAuthInterceptor authInterceptor;
    private final JobQProperties properties;

    public JobQWebMvcConfigurer(JobQAuthInterceptor authInterceptor, JobQProperties properties) {
        this.authInterceptor = authInterceptor;
        this.properties = properties;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        String dashboardPath = normalizeDashboardPath(properties.getDashboard().getPath());
        // Protect both the internal dashboard endpoints and the configured external entry path.
        registry.addInterceptor(authInterceptor)
                .addPathPatterns("/jobq/**", dashboardPath, dashboardPath + "/**");
    }

    private String normalizeDashboardPath(String configuredPath) {
        if (configuredPath == null || configuredPath.isBlank()) {
            return "/jobq/dashboard";
        }
        String normalized = configuredPath.startsWith("/") ? configuredPath : "/" + configuredPath;
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
