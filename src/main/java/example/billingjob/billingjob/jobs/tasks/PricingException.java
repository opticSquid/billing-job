package example.billingjob.billingjob.jobs.tasks;

public class PricingException extends RuntimeException {
    public PricingException(String message) {
        super(message);
    }
}
