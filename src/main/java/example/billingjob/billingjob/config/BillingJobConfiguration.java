package example.billingjob.billingjob.config;

import org.springframework.batch.core.Job;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import example.billingjob.billingjob.jobs.BillingJob;

@Configuration
public class BillingJobConfiguration {
    @Bean
    Job job() {
        return new BillingJob();
    }
}
