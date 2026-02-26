package com.jobq;

import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobWorkerDefaultTypeTest {

    @com.jobq.annotation.Job("ANNOTATED_DEFAULT_TYPE")
    static class AnnotatedWorker implements JobWorker<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }

        @Override
        public Class<String> getPayloadClass() {
            return String.class;
        }
    }

    static class MissingTypeWorker implements JobWorker<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }

        @Override
        public Class<String> getPayloadClass() {
            return String.class;
        }
    }

    @Test
    void shouldResolveJobTypeFromAnnotationByDefault() {
        assertEquals("ANNOTATED_DEFAULT_TYPE", new AnnotatedWorker().getJobType());
    }

    @Test
    void shouldResolveJobTypeFromAnnotationWhenWorkerIsProxied() {
        ProxyFactory proxyFactory = new ProxyFactory(new AnnotatedWorker());
        proxyFactory.setProxyTargetClass(true);

        @SuppressWarnings("unchecked")
        JobWorker<String> proxiedWorker = (JobWorker<String>) proxyFactory.getProxy();
        assertEquals("ANNOTATED_DEFAULT_TYPE", proxiedWorker.getJobType());
    }

    @Test
    void shouldFailWhenWorkerHasNoAnnotationAndNoOverride() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new MissingTypeWorker().getJobType());
        assertTrue(ex.getMessage().contains("must either be annotated with @Job or override getJobType()"));
    }
}
