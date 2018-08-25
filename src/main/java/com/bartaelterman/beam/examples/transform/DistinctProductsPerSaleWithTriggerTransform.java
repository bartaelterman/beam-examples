package com.bartaelterman.beam.examples.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.*;


public class DistinctProductsPerSaleWithTriggerTransform extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

    private static final int WINDOWGAPDURATION = 600;
    private static final int TRIGGERINGTIME = 30;
    static final Logger LOG = LoggerFactory.getLogger(DistinctProductsPerSaleWithTriggerTransform.class);

    // Parse the sale ID and product ID from the string
    static class SaleIdAsKeyFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String productString = c.element();
            LOG.info("Processing " + productString);
            String[] splits = productString.split(" ");
            if (splits.length == 2) {
                String saleID = splits[0];
                String productID = splits[1];
                c.output(KV.of(saleID, productID));
            }
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
                        .withAllowedLateness(Duration.standardSeconds(10)))
                // Apply the custom DistinctProductsFunc. A Combine function that will
                // aggregate all products per window, and return the distinct product IDs
                // per sale.
                .apply("Aggregate",  Combine.perKey(new DistinctProductsFn()));
    }
}
