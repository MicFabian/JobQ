package com.jobq.dashboard;

import com.jobq.config.JobQProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.util.Base64;

@Component
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true")
public class JobQDashboardCredentialsInitializer {

    private static final Logger log = LoggerFactory.getLogger(JobQDashboardCredentialsInitializer.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final JobQProperties properties;

    public JobQDashboardCredentialsInitializer(JobQProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    void initializeCredentials() {
        if (properties.getDashboard().getAuthMode() != JobQProperties.Dashboard.AuthMode.BASIC) {
            return;
        }

        String username = properties.getDashboard().getUsername();
        String password = properties.getDashboard().getPassword();

        if (isBlank(username) || isBlank(password)) {
            String generatedUsername = "jobq-" + randomToken(6);
            String generatedPassword = randomToken(18);
            properties.getDashboard().setUsername(generatedUsername);
            properties.getDashboard().setPassword(generatedPassword);

            log.info(
                    "JobQ dashboard credentials were not fully configured. Generated credentials for '{}': username='{}' password='{}'",
                    properties.getDashboard().getPath(),
                    generatedUsername,
                    generatedPassword);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String randomToken(int bytes) {
        byte[] randomBytes = new byte[bytes];
        SECURE_RANDOM.nextBytes(randomBytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
    }
}
