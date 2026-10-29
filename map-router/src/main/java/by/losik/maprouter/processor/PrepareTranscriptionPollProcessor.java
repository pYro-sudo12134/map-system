package by.losik.maprouter.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PrepareTranscriptionPollProcessor implements Processor {

    @Override
    public void process(@NonNull Exchange exchange) {
        String jobName = exchange.getProperty("jobName", String.class);
        if (jobName == null || jobName.isEmpty()) {
            throw new IllegalStateException("Transcription jobName is not set");
        }
        exchange.getMessage().setBody(null);
        exchange.getMessage().removeHeaders("CamelAwsTranscribe*");
        exchange.getMessage().setHeader("CamelAwsTranscribeTranscriptionJobName", jobName);
        log.debug("Polling transcription job: {}", jobName);
    }
}