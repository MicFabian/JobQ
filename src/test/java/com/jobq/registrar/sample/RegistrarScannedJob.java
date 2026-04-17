package com.jobq.registrar.sample;

import com.jobq.annotation.Job;
import java.util.UUID;

@Job(payload = String.class)
public class RegistrarScannedJob {

    public void process(UUID jobId, String payload) {
        // no-op
    }
}
