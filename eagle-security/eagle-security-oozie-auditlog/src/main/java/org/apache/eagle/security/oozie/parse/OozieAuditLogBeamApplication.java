package org.apache.eagle.security.oozie.parse;

import com.typesafe.config.Config;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.eagle.app.BeamApplication;
import org.apache.eagle.app.environment.impl.BeamEnviroment;

public class OozieAuditLogBeamApplication extends BeamApplication {

    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "eagleConsumer";
    private static final String DEFAULT_TRANSACTION_ZK_ROOT = "/consumers";

    private String configPrefix = DEFAULT_CONFIG_PREFIX;
    @Override
    public Pipeline execute(Config config, BeamEnviroment environment) {

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        // Apply the pipeline's transforms.

        // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
        // of input text files. TextIO.Read returns a PCollection where each element is one line from
        // the input text (a set of Shakespeare's texts).

        // This example reads a public data set consisting of the complete works of Shakespeare.
        p.apply(TextIO.Read.from("/usr/apache-beam/beam/README.md"))

                // Concept #2: Apply a ParDo transform to our PCollection of text lines. This ParDo invokes a
                // DoFn (defined in-line) on each element that tokenizes the text line into individual words.
                // The ParDo returns a PCollection<String>, where each element is an individual word in
                // Shakespeare's collected texts.
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split("[^a-zA-Z']+")) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))

                // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
                // transform returns a new PCollection of key/value pairs, where each key represents a unique
                // word in the text. The associated value is the occurrence count for that word.
                .apply(Count.<String>perElement())

                // Apply a MapElements transform that formats our PCollection of word counts into a printable
                // string, suitable for writing to an output file.
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }))

                // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
                // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
                // formatted strings) to a series of text files.
                //
                // By default, it will write to a set of files with names like wordcount-00001-of-00005
                .apply(TextIO.Write.to("/usr/beam-result/wordcounts"));



        return p;
    }
}
