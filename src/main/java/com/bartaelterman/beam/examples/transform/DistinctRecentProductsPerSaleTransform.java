package com.bartaelterman.beam.examples.transform;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class DistinctRecentProductsPerSaleTransform extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

    private static final int WINDOWGAPDURATION = 600;
    private static final int TRIGGERINGTIME = 30;

    // Parse the sale ID and product ID from the string and set the sale ID as key
    // and the product ID as value
    static class SaleIdAsKeyFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String productString = c.element();
            String[] splits = productString.split(" ");
            if (splits.length == 2) {
                String saleID = splits[0];
                String productID = splits[1];
                c.output(KV.of(saleID, productID));
            }
        }
    }

    // Parse the sale ID and product ID from the string and set the product ID as key
    // and the entire string as value
    static class ProductIdAsKeyFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String productString = c.element();
            String[] splits = productString.split(" ");
            if (splits.length == 2) {
                String saleID = splits[0];
                String productID = splits[1];
                c.output(KV.of(productID, productString));
            }
        }
    }

    // For a KV, drop the key and only return the value
    static class ExtractValue extends DoFn<KV<String, String>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getValue());
        }
    }

    // A simple accumulator. Just keep a set of the product IDs
    public static class Accum implements Serializable {
        HashSet<String> products = new HashSet<>();

        public void set(String productID) {
            if (!products.contains(productID)) {
                products.add(productID);
            }
        }

        public Collection<String> getProducts () {
            return products;
        }
    }

    // Custom Combine function to get the unique product IDs per sale
    public class DistinctProductsFn extends Combine.CombineFn<String, Accum, String> {
        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum accum, String productID) {
            accum.set(productID);
            return accum;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();
            for (Accum accum: accumulators) {
                for (String productID: accum.getProducts()) {
                    merged.set(productID);
                }
            }
            return merged;
        }

        @Override
        public String extractOutput(Accum accumulator) {
            ArrayList<String> l = new ArrayList<>(accumulator.getProducts());
            Collections.sort(l);
            return String.join(",", l);
        }
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
        return input
                // Set the product id as the key, and the initial string as value
                .apply("ProductIdAsKey", ParDo.of(new ProductIdAsKeyFn()))

                // Set session windows on the product id, because we want to get the latest state of the product
                .apply("SessionWindows", Window.<KV<String, String>>into(Sessions.withGapDuration(Duration.standardSeconds(300)))
                        .triggering(Repeatedly.forever(AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(10))
                        ).orFinally(AfterWatermark.pastEndOfWindow()))
                        .accumulatingFiredPanes()
                        .withAllowedLateness(Duration.standardSeconds(100)))

                // Apply a Latest transform, to get the latest state of the product
                .apply(Latest.perKey())

                // Drop the key, which is the product ID, we're now back to the initial strings but we only kept
                // the "latest state" (for strings only containing product IDs, that obviously makes no sense,
                // but imagine slightly more complex data)
                .apply("ExtractLatest", ParDo.of(new ExtractValue()))

                // Set the sale ID as key and the product ID as the value
                .apply("SaleIdAsKey", ParDo.of(new SaleIdAsKeyFn()))

                // Set session windows on the sale ID, because we will want to aggregate all
                // products per sale
                .apply(Window.<KV<String, String>>into(Sessions.withGapDuration(Duration.standardSeconds(WINDOWGAPDURATION)))
                        .triggering(Repeatedly.forever(AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(TRIGGERINGTIME))
                        ).orFinally(AfterWatermark.pastEndOfWindow()))
                        .accumulatingFiredPanes()
                        .withAllowedLateness(Duration.ZERO))

                // Apply the custom DistinctProductsFunc. A Combine function that will
                // aggregate all products per window, and return the distinct product IDs
                // per sale.
                .apply("Aggregate",  Combine.perKey(new DistinctProductsFn()));
    }
}
