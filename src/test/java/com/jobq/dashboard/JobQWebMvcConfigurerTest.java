package com.jobq.dashboard;

import com.jobq.config.JobQProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.config.annotation.InterceptorRegistration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JobQWebMvcConfigurerTest {

    @Test
    void shouldProtectCustomDashboardPathInAdditionToInternalEndpoints() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setPath("/ops/jobq");

        JobQWebMvcConfigurer configurer = new JobQWebMvcConfigurer(mock(JobQAuthInterceptor.class), properties);
        InterceptorRegistry registry = new InterceptorRegistry();
        configurer.addInterceptors(registry);

        @SuppressWarnings("unchecked")
        List<InterceptorRegistration> registrations = (List<InterceptorRegistration>) ReflectionTestUtils.getField(registry,
                "registrations");
        assertEquals(1, registrations.size());

        @SuppressWarnings("unchecked")
        List<String> includePatterns = (List<String>) ReflectionTestUtils.getField(registrations.get(0),
                "includePatterns");

        assertTrue(includePatterns.contains("/jobq/**"));
        assertTrue(includePatterns.contains("/ops/jobq"));
        assertTrue(includePatterns.contains("/ops/jobq/**"));
    }

    @Test
    void shouldFallbackToDefaultDashboardPathWhenBlank() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setPath("   ");

        JobQWebMvcConfigurer configurer = new JobQWebMvcConfigurer(mock(JobQAuthInterceptor.class), properties);
        InterceptorRegistry registry = new InterceptorRegistry();
        configurer.addInterceptors(registry);

        @SuppressWarnings("unchecked")
        List<InterceptorRegistration> registrations = (List<InterceptorRegistration>) ReflectionTestUtils.getField(registry,
                "registrations");
        assertEquals(1, registrations.size());

        @SuppressWarnings("unchecked")
        List<String> includePatterns = (List<String>) ReflectionTestUtils.getField(registrations.get(0),
                "includePatterns");

        assertTrue(includePatterns.contains("/jobq/dashboard"));
        assertTrue(includePatterns.contains("/jobq/dashboard/**"));
    }

    @Test
    void shouldNormalizeConfiguredPathWithoutLeadingOrTrailingSlash() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setPath("ops/jobq/");

        JobQWebMvcConfigurer configurer = new JobQWebMvcConfigurer(mock(JobQAuthInterceptor.class), properties);
        InterceptorRegistry registry = new InterceptorRegistry();
        configurer.addInterceptors(registry);

        @SuppressWarnings("unchecked")
        List<InterceptorRegistration> registrations = (List<InterceptorRegistration>) ReflectionTestUtils.getField(registry,
                "registrations");
        assertEquals(1, registrations.size());

        @SuppressWarnings("unchecked")
        List<String> includePatterns = (List<String>) ReflectionTestUtils.getField(registrations.get(0),
                "includePatterns");

        assertTrue(includePatterns.contains("/ops/jobq"));
        assertTrue(includePatterns.contains("/ops/jobq/**"));
    }
}
