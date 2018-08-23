package com.bartaelterman.beam.examples.transform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestDistinctProductsPerSale {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
    private TestPipelineOptions options = PipelineOptionsFactory.fromArgs(
            "--project=project",
            "--tempLocation=gs://bucket/beam")
            .withValidation().as(TestPipelineOptions.class);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.fromOptions(options);

    @Test
    public void should_emit_early_results_after_receiving_6_events() {
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION)
                )
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION)
                )
                .addElements(TimestampedValue.of("a", SEC_1_DURATION))
                .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(AfterPane.elementCountAtLeast(3))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        // doc: "Triggers allow Beam to emit early results, before all the data in a given window has arrived.
        // For example, emitting after a certain amount of time elapses, or after a certain number of elements arrives."
        // Here we want to emit the results after the window receives at least 3 items. In this case we compute the
        // result for 2 .addElements operations (2 + 4 items). The last .addElements is ignored
        // See the next test to discover what happens if we add only 1 event in .addElements method
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=6");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=6");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void EmittedProducts() {
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeProducts = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("campaign1 product1", new Instant(0)),
                TimestampedValue.of("campaign1 product2", new Instant(0)),
                TimestampedValue.of("campaign1 product3", new Instant(0))
        )
                .advanceWatermarkTo(new Instant(700)) // watermark passes trigger time
//                .advanceProcessingTime(new Duration(40))
        .addElements(
                TimestampedValue.of("campaign1 product9", new Instant(710))
        )
//                .advanceWatermarkTo(new Instant(90)) // watermark passes trigger time
////                .advanceProcessingTime(new Duration(50))
//        .addElements(
//            TimestampedValue.of("campaign1 product2", new Instant(95)),
//            TimestampedValue.of("campaign1 product4", new Instant(100))
//        )
//                .advanceWatermarkTo(new Instant(710))
////                .advanceProcessingTime(new Duration(650))
//                .addElements(
//                        TimestampedValue.of("campaign1 product9", new Instant(715))
//                )
//                .advanceProcessingTime(new Duration(20))
        .advanceWatermarkToInfinity();

        PCollection<KV<String, String>> results = applyDistinctProductsTransform(pipeline, onTimeProducts);

        PAssert.that(results).containsInAnyOrder(
                KV.of("campaign1", "product1,product2,product3"),
                KV.of("campaign1", "product9")
//                KV.of("campaign2", "productxyz")
        );
        pipeline.run().waitUntilFinish();
    }

    private static PCollection<String> applyCounter(Pipeline pipeline, Window<String> window,
                                                    TestStream<String> inputCollection) {
        return pipeline.apply(inputCollection)
                .apply(window)
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String letter) -> KV.of(letter, 1)))
                .apply(Count.perKey())
                .apply(MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> pair) ->
                        pair.getKey() + "=" + pair.getValue()));
    }

    private static PCollection<KV<String, String>> applyDistinctProductsTransform(Pipeline pipeline,
                                                                            TestStream<String> inputCollection) {
        return pipeline.apply(inputCollection)
                .apply(new DistinctProductsPerSaleTransform());
    }
}
