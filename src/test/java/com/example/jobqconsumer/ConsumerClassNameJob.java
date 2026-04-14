package com.example.jobqconsumer;

import com.jobq.annotation.Job;
import java.util.UUID;

@Job(payload = ConsumerClassNameJob.Payload.class, maxRetries = 3)
class ConsumerClassNameJob {

    private final ConsumerNoteRepository consumerNoteRepository;

    ConsumerClassNameJob(ConsumerNoteRepository consumerNoteRepository) {
        this.consumerNoteRepository = consumerNoteRepository;
    }

    public void process(UUID jobId, Payload payload) {
        ConsumerNote note = new ConsumerNote();
        note.setMessage("class:" + payload.message());
        consumerNoteRepository.save(note);
    }

    record Payload(String message) {}
}
