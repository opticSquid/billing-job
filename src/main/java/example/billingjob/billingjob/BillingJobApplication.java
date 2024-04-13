package example.billingjob.billingjob;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BillingJobApplication {

	public static void main(String[] args) throws JobParametersInvalidException, JobExecutionException {
		SpringApplication.run(BillingJobApplication.class, args);
	}
}
