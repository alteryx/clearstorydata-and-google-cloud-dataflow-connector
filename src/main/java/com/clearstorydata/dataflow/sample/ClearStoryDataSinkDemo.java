/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */


package com.clearstorydata.dataflow.sample;

import com.clearstorydata.dataflow.io.ClearStoryDataIO;
import com.clearstorydata.dataflow.io.SinkInfo;
import com.clearstorydata.dataflow.util.GcsUrlSigningUtil;
import com.clearstorydata.resource.dataset.Schema;
import com.clearstorydata.resource.dataset.Schema.Column.Type;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Write;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;


/**
 * ClearStoryDataSinkDemo demonstrates the use of CsdSink.
 */
public class ClearStoryDataSinkDemo {

    static class ConvertLineFn extends DoFn<TableRow, List<String>> {
        private static final long serialVersionUID = 0;
        private final Schema schema;

        public ConvertLineFn(Schema schema) {
            this.schema = schema;
        }

        /**
         * Note that we are assuming that csdSchema's attribute names are the column names
         * of BigQuery table.
         * </p>
         * A mapping between Csd's Schema and input data's schema can be built
         * to remove the coupling.
         *
         * @param ctx the ProcessContext
         * @throws Exception if any error happens
         */
        @Override
        public void processElement(ProcessContext ctx) throws Exception {
            TableRow tr = ctx.element();
            List<String> values = new ArrayList<String>();
            for (Schema.Column attr : schema.getColumns()) {
                if (attr.getName().equals("date_time")) {
                    String year = (String) tr.get("year");
                    String month = (String) tr.get("month");
                    String day = (String) tr.get("day");
                    values.add(year + "-" + month + "-" + day);
                } else {
                    Object value = tr.get(attr.getName());
                    // null-proff the value
                    values.add(value == null ? "null" : value.toString());
                }

            }
            ctx.output(values);
        }
    }

    public static class TableRowTransform extends PTransform<PCollection<TableRow>,
            PCollection<List<String>>> {
        private static final long serialVersionUID = 0;
        private final Schema schema;

        public TableRowTransform() {
            this(null);
        }

        public TableRowTransform(final Schema schema) {
            super();
            this.schema = schema;
        }

        /**
         * Applies this {@code PTransform} on the given TableRow, and returns the splitted string
         * list.
         * <p/>
         * <p/>
         *
         * @param line the TableRow
         * @return string list
         */
        public PCollection<List<String>> apply(PCollection<TableRow> line) {

            PCollection<List<String>> results = line.apply(
                    ParDo.of(new ConvertLineFn(this.schema)));
            return results;
        }

    }

    /**
     * Options supported by {@link ClearStoryDataSinkDemo}.
     * <p/>
     * Inherits standard configuration options.
     */
    public static interface Options extends PipelineOptions {
        static final String WEATHER_SAMPLES_TABLE =
                "clouddataflow-readonly:samples.weather_stations";

        @Description("Table to read from, specified as "
                + "<project_id>:<dataset_id>.<table_id>")
        @Default.String(WEATHER_SAMPLES_TABLE)
        String getInput();


        void setInput(String value);

        @Description("Csd Service Endpoint")
        String getCsdEndPoint();

        void setCsdEndPoint(String value);

        @Description("Data Set Name")
        String getCsdDataSetName();

        void setCsdDataSetName(String value);

        @Description("Data Set Id")
        String getCsdDataSetId();

        void setCsdDataSetId(String value);

        @Description("Csd Intermediate Location")
        String getCsdIntermediateLocation();

        void setCsdIntermediateLocation(String value);

        @Description("Csd API Token")
        String getCsdApiToken();

        void setCsdApiToken(String value);

        @Description("SSL Verification")
        boolean getCsdSslVerification();

        void setCsdSslVerification(boolean value);

        @Description("Path to the PKCS12 Key File")
        String getPrivateKeyPath();

        void setPrivateKeyPath(String value);


        @Description("Gcloud Service Account Email")
        String getServiceAccountEmail();

        void setServiceAccountEmail(String value);


        @Description("Data Set Operation Mode")
        String getCsdCurMode();

        void setCsdCurMode(String value);

    }

    /**
     * A Schema is required for Clearstory Data's ingest process.
     * The schema should describe the data extracted/processed from previous transforms.
     *
     * @return the Schema object
     */
    public static Schema createSchema() {
        Schema schema = new Schema();
        schema = schema
                .addColumn("wban_number", Schema.Column.Type.CATEGORY)
                .addColumn("station_number", Type.CATEGORY)
                .addColumn("date_time", Type.TIME)
                .addColumn("mean_temp", Schema.Column.Type.CATEGORY)
                .addColumn("mean_wind_speed", Schema.Column.Type.CATEGORY);
        return schema;
    }


    /**
     * Main function.
     * An example to for how to use ClearstoryDataIOV2.CsdSink.
     * @param args command line arguments
     */
    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        if (options.getServiceAccountEmail() == null
                || options.getServiceAccountEmail().isEmpty()) {
            throw new IllegalArgumentException("Must specify --serviceAccountEmail");
        } else if (options.getPrivateKeyPath() == null
                || options.getPrivateKeyPath().isEmpty()) {
            throw new IllegalArgumentException("Must specify --privateKeyPath");
        }

        Schema schema = createSchema();
        SinkInfo sinkInfo = SinkInfo.builder()
                .setApiToken(options.getCsdApiToken())
                .setDataSetId(options.getCsdDataSetId())
                .setDataSetName(options.getCsdDataSetName())
                .setEndPoint(options.getCsdEndPoint())
                .setSslVerification(options.getCsdSslVerification())
                .setIntermediateLocation(options
                        .getCsdIntermediateLocation())
                .setKeyStore(
                        GcsUrlSigningUtil.loadKeyStoreFromFile(options.getPrivateKeyPath()))
                .setMode(SinkInfo.Mode.valueOf(options.getCsdCurMode()))
                .setSchema(schema)
                .setServiceAccountEmail(options.getServiceAccountEmail())
                .build();

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(BigQueryIO.Read.named("ReadFromBigQuery").from(options.getInput()))
                .apply(new TableRowTransform(schema))
                .apply(Write.to(ClearStoryDataIO.Write
                                        .withSinkInfo(sinkInfo)
                        ).setName("WriteToCsd")
                );
        pipeline.run();
    }
}

