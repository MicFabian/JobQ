package com.jobq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class JobTypeNamesTest {

    static class SampleJob {}

    @Test
    void shouldUseSimpleClassNameByDefault() {
        assertEquals("SampleJob", JobTypeNames.defaultTypeFor(SampleJob.class));
        assertEquals("SampleJob", JobTypeNames.configuredOrDefault("", SampleJob.class));
        assertEquals("SampleJob", JobTypeNames.configuredOrDefault("   ", SampleJob.class));
    }

    @Test
    void shouldKeepExplicitConfiguredType() {
        assertEquals("CUSTOM_JOB", JobTypeNames.configuredOrDefault("  CUSTOM_JOB  ", SampleJob.class));
    }

    @Test
    void shouldRejectAnonymousClassesWithoutSimpleNames() {
        Class<?> anonymousClass = new Runnable() {
            @Override
            public void run() {}
        }.getClass();

        IllegalStateException error =
                assertThrows(IllegalStateException.class, () -> JobTypeNames.defaultTypeFor(anonymousClass));
        assertTrue(error.getMessage().contains("Declare @Job(\"...\") explicitly"));
    }
}
