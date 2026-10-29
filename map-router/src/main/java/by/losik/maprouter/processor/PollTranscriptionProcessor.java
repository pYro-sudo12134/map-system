package by.losik.maprouter.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJob;

@Slf4j
@Component
public class PollTranscriptionProcessor implements Processor {

    @Override
    public void process(@NonNull Exchange exchange) {
        Object body = exchange.getMessage().getBody();
        log.debug("GetTranscriptionJob body class: {}", body == null ? "null" : body.getClass());

        if (!(body instanceof GetTranscriptionJobResponse response)) {
            log.warn("Unexpected body type from aws2-transcribe: {}",
                    body == null ? "null" : body.getClass());
            exchange.setProperty("transcriptionPending", true);
            return;
        }

        TranscriptionJob job = response.transcriptionJob();
        if (job == null) {
            log.warn("GetTranscriptionJobResponse has no TranscriptionJob");
            exchange.setProperty("transcriptionPending", true);
            return;
        }

        String status = job.transcriptionJobStatusAsString();
        log.debug("Transcription status: {}", status);

        if (status == null) {
            exchange.setProperty("transcriptionPending", true);
            return;
        }

        switch (status) {
            case "COMPLETED" -> {
                String uri = job.transcript() != null ? job.transcript().transcriptFileUri() : null;
                if (uri == null || uri.isEmpty()) {
                    throw new IllegalStateException(
                            "Transcription COMPLETED but TranscriptFileUri is missing");
                }
                exchange.setProperty("transcriptUri", uri);
                exchange.setProperty("transcriptionPending", false);
            }
            case "FAILED" -> {
                String reason = job.failureReason();
                throw new IllegalStateException(
                        "Transcription job failed: " + (reason == null ? "unknown" : reason));
            }
            default -> exchange.setProperty("transcriptionPending", true);
        }
    }
}