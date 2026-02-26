package com.jobq.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.JobRepository;
import com.jobq.config.JobQProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JobPollerOnErrorSignatureTest {

    @Test
    void shouldOnlyAcceptExceptionOrThrowableForOnErrorParameter() {
        JobPoller poller = new JobPoller(
                mock(JobRepository.class),
                List.of(),
                mock(ListableBeanFactory.class),
                new ObjectMapper(),
                mock(TransactionTemplate.class),
                new JobQProperties());

        Boolean acceptsException = ReflectionTestUtils.invokeMethod(poller, "acceptsSupportedOnErrorExceptionType",
                Exception.class);
        Boolean acceptsThrowable = ReflectionTestUtils.invokeMethod(poller, "acceptsSupportedOnErrorExceptionType",
                Throwable.class);
        Boolean acceptsObject = ReflectionTestUtils.invokeMethod(poller, "acceptsSupportedOnErrorExceptionType",
                Object.class);

        assertTrue(Boolean.TRUE.equals(acceptsException));
        assertTrue(Boolean.TRUE.equals(acceptsThrowable));
        assertFalse(Boolean.TRUE.equals(acceptsObject));
    }
}
