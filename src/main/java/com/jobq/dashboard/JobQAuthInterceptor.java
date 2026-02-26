package com.jobq.dashboard;

import com.jobq.config.JobQProperties;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
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
        if (properties.getDashboard().getAuthMode() == JobQProperties.Dashboard.AuthMode.SPRING_SECURITY) {
            return authorizeWithSpringSecurity(request, response);
        }

        String reqUsername = properties.getDashboard().getUsername();
        String reqPassword = properties.getDashboard().getPassword();

        // Dashboard must never be open. If credentials are missing for any reason,
        // fail closed and request authentication.
        if (reqUsername == null || reqUsername.isBlank() || reqPassword == null || reqPassword.isBlank()) {
            response.setHeader("WWW-Authenticate", "Basic realm=\"JobQ Dashboard\"");
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }

        String authHeader = request.getHeader("Authorization");

        if (authHeader != null && authHeader.regionMatches(true, 0, "Basic ", 0, "Basic ".length())) {
            String base64Credentials = authHeader.substring("Basic ".length()).trim();
            try {
                byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
                String credentials = new String(credDecoded, StandardCharsets.UTF_8);

                final String[] values = credentials.split(":", 2);
                if (values.length == 2) {
                    String username = values[0];
                    String password = values[1];

                    if (constantTimeEquals(reqUsername, username) && constantTimeEquals(reqPassword, password)) {
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

    private boolean authorizeWithSpringSecurity(HttpServletRequest request, HttpServletResponse response) {
        Principal principal = request.getUserPrincipal();
        if (principal == null) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }

        String requiredRole = properties.getDashboard().getRequiredRole();
        if (requiredRole == null || requiredRole.isBlank()) {
            return true;
        }

        if (hasRole(request, requiredRole)) {
            return true;
        }

        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        return false;
    }

    private boolean hasRole(HttpServletRequest request, String requiredRole) {
        if (request.isUserInRole(requiredRole)) {
            return true;
        }
        if (requiredRole.startsWith("ROLE_")) {
            return request.isUserInRole(requiredRole.substring("ROLE_".length()));
        }
        return request.isUserInRole("ROLE_" + requiredRole);
    }

    private boolean constantTimeEquals(String left, String right) {
        return MessageDigest.isEqual(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }
}
