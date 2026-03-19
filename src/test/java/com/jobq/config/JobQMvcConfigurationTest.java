package com.jobq.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;

class JobQMvcConfigurationTest {

    @Test
    void shouldFallbackToDefaultDashboardPathWhenBlank() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setPath("   ");

        JobQMvcConfiguration configuration = new JobQMvcConfiguration(properties);
        ViewControllerRegistry registry = new ViewControllerRegistry(new StaticApplicationContext());
        configuration.addViewControllers(registry);

        @SuppressWarnings("unchecked")
        List<ViewControllerRegistration> registrations =
                (List<ViewControllerRegistration>) ReflectionTestUtils.getField(registry, "registrations");

        assertEquals(1, registrations.size());
        assertEquals("/jobq/dashboard", ReflectionTestUtils.getField(registrations.get(0), "urlPath"));
    }

    @Test
    void shouldNormalizeDashboardPathWithoutLeadingOrTrailingSlash() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setPath("ops/jobq/");

        JobQMvcConfiguration configuration = new JobQMvcConfiguration(properties);
        ViewControllerRegistry registry = new ViewControllerRegistry(new StaticApplicationContext());
        configuration.addViewControllers(registry);

        @SuppressWarnings("unchecked")
        List<ViewControllerRegistration> registrations =
                (List<ViewControllerRegistration>) ReflectionTestUtils.getField(registry, "registrations");

        assertEquals(1, registrations.size());
        assertEquals("/ops/jobq", ReflectionTestUtils.getField(registrations.get(0), "urlPath"));
    }
}
