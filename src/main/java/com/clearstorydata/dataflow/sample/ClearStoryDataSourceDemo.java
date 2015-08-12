/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.sample;

import com.clearstorydata.dataflow.io.ClearStoryDataIO;
import com.clearstorydata.dataflow.io.ClearStoryDataIO.ClearStoryDataSource;
import com.clearstorydata.dataflow.io.SourceInfo;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.List;

/**
 * SourceDemo demonstrates the use of CsdSource.
 */
public class ClearStoryDataSourceDemo {

    static class MergeCellFn extends DoFn<List<String>, String> {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext ctx) throws Exception {
            List<String> in = ctx.element();
            StringBuffer sb = new StringBuffer();

            for (String v : in) {
                sb.append(v).append("|");
            }
            ctx.output(sb.toString());
        }
    }

    /**
     * A MergeCellTransform is an operation that merges a
     * PCollection containing list of string into a PCollection of
     * a flattened string.
     */
    public static class MergeCellTransform extends PTransform<PCollection<List<String>>,
            PCollection<String>> {
        private static final long serialVersionUID = 0;

        /**
         * Applies this {@code PTransform} on the given list of strings, and returns its
         * a flattened string.
         * <p/>
         * @param lines list of strings
         * @return flattened string
         */
        public PCollection<String> apply(PCollection<List<String>> lines) {

            PCollection<String> results = lines.apply(
                    ParDo.of(new MergeCellFn())
            );
            return results;
        }

    }

    /**
     * Options supported by {@link ClearStoryDataSourceDemo}.
     * <p/>
     * Inherits standard configuration options.
     */
    public static interface Options extends PipelineOptions {
        @Description("Csd EndPoint to read from")
        String getCsdEndPoint();

        void setCsdEndPoint(String value);

        @Description("Path of the file to write to")
        String getOutput();

        void setOutput(String value);


        @Description("Data Set Id")
        String getCsdDataSetId();

        void setCsdDataSetId(String value);

        @Description("Csd API Token")
        String getCsdApiToken();

        void setCsdApiToken(String value);

        @Description("SSL Verification")
        boolean getCsdSslVerification();

        void setCsdSslVerification(boolean value);

    }

    /**
     * Main function.
     *
     * <p> An example to for how to use ClearstoryDataIOV2.CsdSource.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        SourceInfo sourceInfo = SourceInfo.builder()
                .setApiToken(options.getCsdApiToken())
                .setDataSetId(options.getCsdDataSetId())
                .setEndPoint(options.getCsdEndPoint())
                .setSslVerification(options.getCsdSslVerification())
                .build();

        ClearStoryDataSource csdSource = ClearStoryDataIO
                .Read
                .withSourceInfo(sourceInfo);

        TextIO.Write.Bound<String> textWrite = TextIO
                .Write
                .named("TextIOWriteLines")
                .to(options.getOutput())
                .withSuffix(".csv")
                .withNumShards(1);

        pipeline.apply(Read.from(csdSource).named("ReadFromCsd"))
                .apply(new MergeCellTransform())
                .apply(textWrite);
        pipeline.run();
    }
}

