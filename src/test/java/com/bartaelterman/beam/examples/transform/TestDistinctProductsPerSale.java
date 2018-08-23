package com.bartaelterman.beam.examples.transform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestDistinctProductsPerSale {

    private static final Instant NOW = new Instant(0);
    private TestPipelineOptions options = PipelineOptionsFactory.fromArgs(
            "--project=project",
            "--tempLocation=gs://bucket/beam")
            .withValidation().as(TestPipelineOptions.class);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.fromOptions(options);

    @Test
    public void TestSessionWindow() {
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeProducts = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("campaign1 product1", new Instant(0)),
                TimestampedValue.of("campaign1 product2", new Instant(0)),
                TimestampedValue.of("campaign1 product3", new Instant(0))
        )
                .advanceWatermarkTo(new Instant(700)) // watermark passes trigger time
        .addElements(
                TimestampedValue.of("campaign1 product9", new Instant(710))
        )
        .advanceWatermarkToInfinity();

        PCollection<KV<String, String>> results = applyDistinctProductsTransform(pipeline, onTimeProducts);

        PAssert.that(results).containsInAnyOrder(
                KV.of("campaign1", "product1,product2,product3"),
                KV.of("campaign1", "product9")
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void TestSessionsWithTriggers() {
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeProducts = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("campaign1 product1", new Instant(0)),
                TimestampedValue.of("campaign1 product2", new Instant(0)),
                TimestampedValue.of("campaign1 product3", new Instant(0))
        )
                .advanceWatermarkTo(new Instant(700)) // watermark passes trigger time
                .advanceProcessingTime(new Duration(40))
        .addElements(
                TimestampedValue.of("campaign1 product9", new Instant(710))
        )
                .advanceWatermarkTo(new Instant(90)) // watermark passes trigger time
                .advanceProcessingTime(new Duration(50))
        .addElements(
            TimestampedValue.of("campaign1 product2", new Instant(95)),
            TimestampedValue.of("campaign1 product4", new Instant(100))
        )
                .advanceWatermarkTo(new Instant(710))
                .advanceProcessingTime(new Duration(650))
                .addElements(
                        TimestampedValue.of("campaign1 product9", new Instant(715))
                )
                .advanceProcessingTime(new Duration(20))
        .advanceWatermarkToInfinity();

        PCollection<KV<String, String>> results = applyDistinctProductsTransform(pipeline, onTimeProducts);

        PAssert.that(results).containsInAnyOrder(
                KV.of("campaign1", "product1,product2,product3"),
                KV.of("campaign1", "product9"),
                KV.of("campaign2", "productxyz")
        );
        pipeline.run().waitUntilFinish();
    }

    private static PCollection<KV<String, String>> applyDistinctProductsTransform(Pipeline pipeline,
                                                                            TestStream<String> inputCollection) {
        return pipeline.apply(inputCollection)
                .apply(new DistinctProductsPerSaleTransform());
    }
}
