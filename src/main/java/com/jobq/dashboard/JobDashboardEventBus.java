package com.jobq.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true")
public class JobDashboardEventBus {

    private static final Logger log = LoggerFactory.getLogger(JobDashboardEventBus.class);
    private static final long EMITTER_TIMEOUT_MS = 30L * 60L * 1000L;

    private final ObjectMapper objectMapper;
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public JobDashboardEventBus(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SseEmitter createEmitter() {
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(error -> emitters.remove(emitter));
        sendSafely(emitter, "connected", Map.of("ok", true));
        return emitter;
    }

    public void publishRefresh() {
        publish("refresh", Map.of("scope", "dashboard"));
    }

    public void publishJobRuntime(UUID jobId) {
        if (jobId == null) {
            return;
        }
        publish("job-runtime", Map.of("jobId", jobId.toString()));
    }

    private void publish(String eventName, Object payload) {
        for (SseEmitter emitter : emitters) {
            sendSafely(emitter, eventName, payload);
        }
    }

    private void sendSafely(SseEmitter emitter, String eventName, Object payload) {
        try {
            emitter.send(SseEmitter.event().name(eventName).data(objectMapper.writeValueAsString(payload)));
        } catch (IOException ioException) {
            emitters.remove(emitter);
            emitter.completeWithError(ioException);
            log.trace("Removing failed JobQ dashboard SSE emitter: {}", ioException.getMessage());
        }
    }
}
