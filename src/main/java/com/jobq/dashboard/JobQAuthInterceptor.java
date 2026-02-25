package com.jobq.dashboard;

import com.jobq.config.JobQProperties;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Component
public class JobQAuthInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(JobQAuthInterceptor.class);
    private final JobQProperties properties;

    public JobQAuthInterceptor(JobQProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String reqUsername = properties.getDashboard().getUsername();
        String reqPassword = properties.getDashboard().getPassword();

        // If credentials are not set, auth is bypassed (or managed globally by the host
        // app)
        if (reqUsername == null || reqUsername.isBlank() || reqPassword == null || reqPassword.isBlank()) {
            return true;
        }

        String authHeader = request.getHeader("Authorization");

        if (authHeader != null && authHeader.toLowerCase().startsWith("basic ")) {
            String base64Credentials = authHeader.substring("Basic ".length()).trim();
            try {
                byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
                String credentials = new String(credDecoded, StandardCharsets.UTF_8);

                final String[] values = credentials.split(":", 2);
                if (values.length == 2) {
                    String username = values[0];
                    String password = values[1];

                    // Standard string compare; acceptable for local library scopes without Spring
                    // Security
                    if (reqUsername.equals(username) && reqPassword.equals(password)) {
                        return true;
                    }
                }
            } catch (IllegalArgumentException e) {
                log.warn("Invalid Base64 encoded Basic Auth credentials for JobQ Dashboard.");
            }
        }

        // Return 401 Unauthorized with WWW-Authenticate header
        response.setHeader("WWW-Authenticate", "Basic realm=\"JobQ Dashboard\"");
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        return false;
    }
}
