package com.jobq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.config.JobQProperties;
import org.junit.jupiter.api.Test;

class JobPayloadRedactorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldRedactConfiguredFieldsRecursivelyAndCaseInsensitively() throws Exception {
        JobQProperties properties = new JobQProperties();
        properties.getDashboard().setRedactedPayloadFields(java.util.List.of("password", "Token"));
        JobPayloadRedactor redactor = new JobPayloadRedactor(properties);

        JsonNode payload = OBJECT_MAPPER.readTree(
                """
                {
                  "email": "user@example.com",
                  "password": "secret",
                  "nested": {
                    "token": "abc123",
                    "safe": "ok"
                  },
                  "items": [
                    { "Password": "other-secret" },
                    { "safe": true }
                  ]
                }
                """);

        JsonNode redacted = redactor.redact(payload);

        assertEquals("user@example.com", redacted.get("email").asText());
        assertEquals("[REDACTED]", redacted.get("password").asText());
        assertEquals("[REDACTED]", redacted.path("nested").path("token").asText());
        assertEquals("ok", redacted.path("nested").path("safe").asText());
        assertEquals(
                "[REDACTED]", redacted.path("items").get(0).path("Password").asText());
        assertEquals(true, redacted.path("items").get(1).path("safe").asBoolean());
    }

    @Test
    void shouldReturnOriginalPayloadWhenNoFieldsAreConfigured() throws Exception {
        JobPayloadRedactor redactor = new JobPayloadRedactor(new JobQProperties());
        JsonNode payload = OBJECT_MAPPER.readTree("{\"email\":\"user@example.com\"}");

        assertSame(payload, redactor.redact(payload));
    }

    @Test
    void shouldHandleNullPayload() {
        JobPayloadRedactor redactor = new JobPayloadRedactor(new JobQProperties());

        assertNull(redactor.redact(null));
    }
}
