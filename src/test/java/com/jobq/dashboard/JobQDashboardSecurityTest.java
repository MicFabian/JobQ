package com.jobq.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.JobRepository;
import com.jobq.config.JobQProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Base64;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobQDashboardSecurityTest {

    private MockMvc mockMvc;
    private JobRepository jobRepository;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        when(jobRepository.countLifecycleCounts()).thenReturn(zeroCounts());

        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setUsername("admin");
        properties.getDashboard().setPassword("supersecret");
        this.mockMvc = buildMockMvc(properties);
    }

    private MockMvc buildMockMvc(JobQProperties properties) {
        ObjectMapper objectMapper = new ObjectMapper();
        JobQDashboardController controller = new JobQDashboardController(jobRepository, objectMapper);
        JobQAuthInterceptor authInterceptor = new JobQAuthInterceptor(properties);
        return MockMvcBuilders.standaloneSetup(controller)
                .addInterceptors(authInterceptor)
                .build();
    }

    private JobRepository.LifecycleCounts zeroCounts() {
        return new JobRepository.LifecycleCounts() {
            @Override
            public Long getPendingCount() {
                return 0L;
            }

            @Override
            public Long getProcessingCount() {
                return 0L;
            }

            @Override
            public Long getCompletedCount() {
                return 0L;
            }

            @Override
            public Long getFailedCount() {
                return 0L;
            }
        };
    }

    @Test
    void shouldReturnUnauthorizedWithoutCredentials() throws Exception {
        mockMvc.perform(get("/jobq/htmx/stats"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"JobQ Dashboard\""));
    }

    @Test
    void shouldReturnUnauthorizedWithInvalidCredentials() throws Exception {
        String base64Creds = Base64.getEncoder().encodeToString("admin:wrongpassword".getBytes());

        mockMvc.perform(get("/jobq/htmx/stats")
                .header(HttpHeaders.AUTHORIZATION, "Basic " + base64Creds))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void shouldReturnOkWithValidCredentials() throws Exception {
        String base64Creds = Base64.getEncoder().encodeToString("admin:supersecret".getBytes());

        mockMvc.perform(get("/jobq/htmx/stats")
                .header(HttpHeaders.AUTHORIZATION, "Basic " + base64Creds))
                .andExpect(status().isOk());
    }

    @Test
    void shouldFailClosedWhenCredentialsAreMissing() throws Exception {
        JobQProperties properties = new JobQProperties();
        MockMvc missingCredsMvc = buildMockMvc(properties);

        missingCredsMvc.perform(get("/jobq/htmx/stats"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"JobQ Dashboard\""));
    }

    @Test
    void shouldRequireAuthenticationInSpringSecurityMode() throws Exception {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setAuthMode(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY);
        MockMvc mvc = buildMockMvc(properties);

        mvc.perform(get("/jobq/htmx/stats"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void shouldReturnForbiddenWhenUserLacksRequiredRoleInSpringSecurityMode() throws Exception {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setAuthMode(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY);
        properties.getDashboard().setRequiredRole("JOBQ_DASHBOARD");
        MockMvc mvc = buildMockMvc(properties);

        mvc.perform(get("/jobq/htmx/stats").with(request -> {
            request.setUserPrincipal(() -> "alice");
            request.addUserRole("USER");
            return request;
        })).andExpect(status().isForbidden());
    }

    @Test
    void shouldAllowUserWithConfiguredRoleInSpringSecurityMode() throws Exception {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setAuthMode(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY);
        properties.getDashboard().setRequiredRole("JOBQ_DASHBOARD");
        MockMvc mvc = buildMockMvc(properties);

        mvc.perform(get("/jobq/htmx/stats").with(request -> {
            request.setUserPrincipal(() -> "alice");
            request.addUserRole("JOBQ_DASHBOARD");
            return request;
        })).andExpect(status().isOk());
    }

    @Test
    void shouldAllowRoleConfiguredWithRolePrefix() throws Exception {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setAuthMode(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY);
        properties.getDashboard().setRequiredRole("ROLE_JOBQ_DASHBOARD");
        MockMvc mvc = buildMockMvc(properties);

        mvc.perform(get("/jobq/htmx/stats").with(request -> {
            request.setUserPrincipal(() -> "alice");
            request.addUserRole("JOBQ_DASHBOARD");
            return request;
        })).andExpect(status().isOk());
    }

    @Test
    void shouldAllowUserRoleWithRolePrefixWhenRequiredRoleIsBare() throws Exception {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setAuthMode(JobQProperties.Dashboard.AuthMode.SPRING_SECURITY);
        properties.getDashboard().setRequiredRole("JOBQ_DASHBOARD");
        MockMvc mvc = buildMockMvc(properties);

        mvc.perform(get("/jobq/htmx/stats").with(request -> {
            request.setUserPrincipal(() -> "alice");
            request.addUserRole("ROLE_JOBQ_DASHBOARD");
            return request;
        })).andExpect(status().isOk());
    }
}
