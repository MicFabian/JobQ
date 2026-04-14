package com.example.jobqconsumer;

import com.jobq.annotation.Job;
import java.util.UUID;

@Job(value = "CONSUMER_AUTO_CONFIG_JOB", payload = ConsumerAutoConfigJob.Payload.class, maxRetries = 3)
class ConsumerAutoConfigJob {

    private final ConsumerNoteRepository consumerNoteRepository;

    ConsumerAutoConfigJob(ConsumerNoteRepository consumerNoteRepository) {
        this.consumerNoteRepository = consumerNoteRepository;
    }

    public void process(UUID jobId, Payload payload) {
        ConsumerNote note = new ConsumerNote();
        note.setMessage("explicit:" + payload.message());
        consumerNoteRepository.save(note);
    }

    record Payload(String message) {}
}
