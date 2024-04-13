package example.billingjob.billingjob.jobs.tasks;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;

import example.billingjob.billingjob.entity.BillingData;
import example.billingjob.billingjob.entity.ReportingData;

public class BillingDataProcessor implements ItemProcessor<BillingData, ReportingData> {
    private final PricingService pricingService;

    public BillingDataProcessor(PricingService pricingService) {
        this.pricingService = pricingService;
    }

    @Value("${spring.cellular.spending.threshold:150}")
    private float spendingThreshold;

    @Override
    public ReportingData process(BillingData item) {
        double billingTotal = item.dataUsage() * pricingService.getDataPricing() +
                item.callDuration() * pricingService.getCallPricing() +
                item.smsCount() * pricingService.getSmsPricing();
        if (billingTotal < spendingThreshold) {
            return null;
        }
        return new ReportingData(item, billingTotal);
    }
}
