package com.jobq.dashboard;

import com.jobq.config.JobQProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobQDashboardCredentialsInitializerTest {

    @Test
    void shouldGenerateCredentialsWhenMissing() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setUsername("");
        properties.getDashboard().setPassword("");

        JobQDashboardCredentialsInitializer initializer = new JobQDashboardCredentialsInitializer(properties);
        initializer.initializeCredentials();

        assertFalse(properties.getDashboard().getUsername().isBlank());
        assertFalse(properties.getDashboard().getPassword().isBlank());
        assertFalse(properties.getDashboard().getUsername().equals(properties.getDashboard().getPassword()));
    }

    @Test
    void shouldKeepConfiguredCredentialsUntouched() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setUsername("admin");
        properties.getDashboard().setPassword("supersecret");

        JobQDashboardCredentialsInitializer initializer = new JobQDashboardCredentialsInitializer(properties);
        initializer.initializeCredentials();

        assertEquals("admin", properties.getDashboard().getUsername());
        assertEquals("supersecret", properties.getDashboard().getPassword());
    }

    @Test
    void shouldRegenerateBothCredentialsWhenOnlyOneIsMissing() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setUsername("configured-user");
        properties.getDashboard().setPassword("");

        JobQDashboardCredentialsInitializer initializer = new JobQDashboardCredentialsInitializer(properties);
        initializer.initializeCredentials();

        assertNotEquals("configured-user", properties.getDashboard().getUsername());
        assertFalse(properties.getDashboard().getUsername().isBlank());
        assertFalse(properties.getDashboard().getPassword().isBlank());
    }

    @Test
    void shouldNotGenerateCredentialsWhenSpringSecurityModeIsUsed() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setAuthMode(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY);
        properties.getDashboard().setUsername("");
        properties.getDashboard().setPassword("");

        JobQDashboardCredentialsInitializer initializer = new JobQDashboardCredentialsInitializer(properties);
        initializer.initializeCredentials();

        assertTrue(properties.getDashboard().getUsername().isBlank());
        assertTrue(properties.getDashboard().getPassword().isBlank());
    }
}
