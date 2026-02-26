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
    }

    static class MissingTypeWorker implements JobWorker<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static class RawWorker implements JobWorker {
        @Override
        public void process(UUID jobId, Object payload) {
            // no-op
        }
    }

    @Test
    void shouldResolveJobTypeFromAnnotationByDefault() {
        assertEquals("ANNOTATED_DEFAULT_TYPE", new AnnotatedWorker().getJobType());
    }

    @Test
    void shouldInferPayloadClassFromGenericByDefault() {
        assertEquals(String.class, new AnnotatedWorker().getPayloadClass());
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
    void shouldInferPayloadClassFromGenericWhenWorkerIsProxied() {
        ProxyFactory proxyFactory = new ProxyFactory(new AnnotatedWorker());
        proxyFactory.setProxyTargetClass(true);

        @SuppressWarnings("unchecked")
        JobWorker<String> proxiedWorker = (JobWorker<String>) proxyFactory.getProxy();
        assertEquals(String.class, proxiedWorker.getPayloadClass());
    }

    @Test
    void shouldFailWhenWorkerHasNoAnnotationAndNoOverride() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new MissingTypeWorker().getJobType());
        assertTrue(ex.getMessage().contains("must either be annotated with @Job or override getJobType()"));
    }

    @Test
    void shouldFailWhenPayloadTypeCannotBeInferred() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new RawWorker().getPayloadClass());
        assertTrue(ex.getMessage().contains("payload type cannot be inferred"));
    }
}
