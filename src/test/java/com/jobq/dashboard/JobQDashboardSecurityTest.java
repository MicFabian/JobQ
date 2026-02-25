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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobQDashboardSecurityTest {

    private MockMvc mockMvc;
    private JobRepository jobRepository;

    @BeforeEach
    void setUp() {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setUsername("admin");
        properties.getDashboard().setPassword("supersecret");

        jobRepository = mock(JobRepository.class);
        ObjectMapper objectMapper = new ObjectMapper();

        JobQDashboardController controller = new JobQDashboardController(jobRepository, objectMapper);
        JobQAuthInterceptor authInterceptor = new JobQAuthInterceptor(properties);

        this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .addInterceptors(authInterceptor)
                .build();
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
}
