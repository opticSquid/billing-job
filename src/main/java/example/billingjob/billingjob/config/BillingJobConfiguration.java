package example.billingjob.billingjob.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.support.JdbcTransactionManager;

import example.billingjob.billingjob.entity.BillingData;
import example.billingjob.billingjob.entity.ReportingData;
import example.billingjob.billingjob.jobs.tasks.BillingDataProcessor;
import example.billingjob.billingjob.jobs.tasks.BillingDataSkipListener;
import example.billingjob.billingjob.jobs.tasks.FilePreparationTasklet;
import example.billingjob.billingjob.jobs.tasks.PricingException;
import example.billingjob.billingjob.jobs.tasks.PricingService;

@Configuration
public class BillingJobConfiguration {
        @Bean
        Job job(JobRepository jobRepository, Step step1, Step step2, Step step3) {
                return new JobBuilder("BillingJob", jobRepository)
                                .start(step1)
                                .next(step2)
                                .next(step3)
                                .build();
        }

        @Bean
        Step step1(JobRepository jobRepository, JdbcTransactionManager transactionManager) {
                return new StepBuilder("filePreparation", jobRepository)
                                .tasklet(new FilePreparationTasklet(), transactionManager).build();
        }

        @Bean
        Step step2(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                        ItemReader<BillingData> billingDataFileReader, ItemWriter<BillingData> billingDataTableWriter,
                        BillingDataSkipListener skipListener) {
                return new StepBuilder("fileIngestion", jobRepository)
                                .<BillingData, BillingData>chunk(100, transactionManager)
                                .reader(billingDataFileReader)
                                .writer(billingDataTableWriter)
                                // if error occours handles it
                                .faultTolerant()
                                // skip the current item(in this case current row in the file) when this
                                // exception occours
                                .skip(FlatFileParseException.class)
                                // if skips are larger than 10 fail the whole job
                                // this means there is some inherent flaw that needs deeper analysis
                                .skipLimit(10)
                                // this listner listens for skips and does operations on them
                                // in this case writes them to a specific file
                                .listener(skipListener)
                                .build();
        }

        @Bean
        Step step3(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                        ItemReader<BillingData> billingDataTableReader,
                        ItemProcessor<BillingData, ReportingData> billingDataProcessor,
                        ItemWriter<ReportingData> billingDataFileWriter) {
                return new StepBuilder("reportGeneration", jobRepository)
                                .<BillingData, ReportingData>chunk(100, transactionManager)
                                .reader(billingDataTableReader)
                                .processor(billingDataProcessor)
                                .writer(billingDataFileWriter)
                                // makes the step tolerate an error
                                .faultTolerant()
                                // in case of error of type PricingExceptio retry
                                .retry(PricingException.class)
                                // retry maximum 100 times no more than that
                                .retryLimit(100)
                                .build();
        }

        /**
         * 
         * @param inputFile - This uses SpEL(Spring Expression Language) to infer its
         *                  value from the comman line arguements in run time
         * 
         * @return
         */
        @Bean
        @StepScope
        FlatFileItemReader<BillingData> billingDataFileReader(
                        @Value("#{jobParameters['input.file']}") String inputFile) {
                return new FlatFileItemReaderBuilder<BillingData>()
                                .name("billingDataFileReader")
                                .resource(new FileSystemResource(inputFile))
                                .delimited()
                                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration",
                                                "smsCount")
                                .targetType(BillingData.class)
                                .build();
        }

        @Bean
        JdbcBatchItemWriter<BillingData> billingDataTableWriter(DataSource dataSource) {
                String sql = "insert into BILLING_DATA values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
                return new JdbcBatchItemWriterBuilder<BillingData>()
                                .dataSource(dataSource)
                                .sql(sql)
                                .beanMapped()
                                .build();
        }

        @Bean
        @StepScope
        JdbcCursorItemReader<BillingData> billingDataTableReader(DataSource dataSource,
                        @Value("#{jobParameters['data.year']}") Integer year,
                        @Value("#{jobParameters['data.month']}") Integer month) {
                String sql = String.format("select * from BILLING_DATA where DATA_YEAR = %d and DATA_MONTH = %d", year,
                                month);
                return new JdbcCursorItemReaderBuilder<BillingData>()
                                .name("billingDataTableReader")
                                .dataSource(dataSource)
                                .sql(sql)
                                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                                .build();
        }

        @Bean
        BillingDataProcessor billingDataProcessor(PricingService pricingService) {
                return new BillingDataProcessor(pricingService);
        }

        @Bean
        @StepScope
        FlatFileItemWriter<ReportingData> billingDataFileWriter(
                        @Value("#{jobParameters['output.file']}") String outputFile) {
                return new FlatFileItemWriterBuilder<ReportingData>()
                                .resource(new FileSystemResource(outputFile))
                                .name("billingDataFileWriter")
                                .delimited()
                                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId",
                                                "billingData.phoneNumber", "billingData.dataUsage",
                                                "billingData.callDuration",
                                                "billingData.smsCount", "billingTotal")
                                .build();
        }

        @Bean
        @StepScope
        BillingDataSkipListener skipListener(@Value("#{jobParameters['skip.file']}") String skippedFile) {
                return new BillingDataSkipListener(skippedFile);
        }

        @Bean
        PricingService pricingService() {
                return new PricingService();
        }
}
