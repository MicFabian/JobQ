package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.jobq.config.JobQProperties;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
public class JobClient {

    private static final Logger log = LoggerFactory.getLogger(JobClient.class);
    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;
    private final JobQProperties properties;

    public JobClient(JobRepository jobRepository, ObjectMapper objectMapper, JobQProperties properties) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public UUID enqueue(String type, Object payload) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries());
    }

    public UUID enqueue(String type, Object payload, int maxRetries) {
        UUID jobId = UUID.randomUUID();
        JsonNode jsonNode = null;
        if (payload != null) {
            jsonNode = objectMapper.valueToTree(payload);
        }
        Job job = new Job(jobId, type, jsonNode, maxRetries, 0);
        job.setUpdatedAt(OffsetDateTime.now());

        jobRepository.save(job);
        log.debug("Enqueued job {} of type {}", jobId, type);
        return jobId;
    }
}
