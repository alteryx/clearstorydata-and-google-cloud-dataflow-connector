/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.io;

import com.clearstorydata.ClearStoryDataClient;
import com.clearstorydata.dataflow.io.SinkInfo.Mode;
import com.clearstorydata.dataflow.util.GcsFileUtil;
import com.clearstorydata.dataflow.util.GcsUrlSigningUtil;
import com.clearstorydata.resource.DataSet;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.io.ShardNameTemplate;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static com.clearstorydata.dataflow.io.SinkInfo.Mode.CREATE;

/**
 * Created by ianlcsd on 6/5/15.
 */
public class ClearStoryDataIO {
    private static final String MISSING_API_TOEKN = "Missing Api Token";
    private static final String MISSING_MODE = "Missing Mode";
    private static final String MISSING_API_TOEKN_CSD_ENDPOINT =
            "Missing ClearStory Data Service End Point";
    private static final String MISSING_CSD_INTERMEDIATE =
            "Missing ClearStory Data intermediate location";
    private static final String MISSING_KEY_STORE = "Missing Key Store";

    private static final String MISSING_SVC_EMAIL = "Missing Service Account Email";
    private static final String MISSING_SCHEMA = "Missing Schema";
    private static final String MISSING_DATASET_NAME = "Missing Data Set Name";
    private static final String MISSING_DATASET_ID = "Missing Data Set Id";
    private static final String MISSING_SINK_INFO = "Missing SinkInfo";
    private static final String MISSING_SOURCE_INFO =  "Missing SourceInfo";
    private static final int GCS_FILE_COMPOE_BATCH_SIZE =  32;



    public static class Write {
        public static ClearStoryDataSink withSinkInfo(SinkInfo sinkInfo) {
            return ClearStoryDataSink.withSinkInfo(sinkInfo);
        }
    }

    public static class ClearStoryDataSink extends FileBasedSink<List<String>> {
        private static final long serialVersionUID = 0;
        private static final Logger LOG = LoggerFactory.getLogger(ClearStoryDataSink.class);
        public static String extension = "csv";
        private final SinkInfo sinkInfo;
        private final Format<List<String>> format = Format.ofCsv();

        public static ClearStoryDataSink withSinkInfo(SinkInfo sinkInfo) {
            return new ClearStoryDataSink(sinkInfo);
        }

        protected ClearStoryDataSink(SinkInfo sinkInfo) {
            super(sinkInfo.getCsdSinkTop(),
                    extension,
                    ShardNameTemplate.DIRECTORY_CONTAINER);
            this.sinkInfo = sinkInfo;
        }

        @Override
        public void validate(PipelineOptions options) {
            super.validate(options);
            Preconditions.checkNotNull(sinkInfo, MISSING_SINK_INFO);
            Preconditions.checkNotNull(sinkInfo.getApiToken(), MISSING_API_TOEKN);
            Preconditions.checkNotNull(sinkInfo.getEndPoint(), MISSING_API_TOEKN_CSD_ENDPOINT);
            Preconditions.checkNotNull(sinkInfo.getIntermediateLocation(),
                    MISSING_CSD_INTERMEDIATE);
            Preconditions.checkNotNull(sinkInfo.getMode(), MISSING_MODE);
            Preconditions.checkNotNull(sinkInfo.getServiceAccountEmail(), MISSING_SVC_EMAIL);
            Preconditions.checkNotNull(sinkInfo.getKeyStore(), MISSING_KEY_STORE);
            Preconditions.checkNotNull(sinkInfo.getSchema(), MISSING_SCHEMA);
            if ( sinkInfo.getMode() == CREATE) {
                Preconditions.checkNotNull(sinkInfo.getDataSetName(), MISSING_DATASET_NAME);
            } else {
                Preconditions.checkNotNull(sinkInfo.getDataSetId(), MISSING_DATASET_ID);
            }

        }

        @Override
        public ClearStoryDataWriteOperation createWriteOperation(PipelineOptions options) {
            return new ClearStoryDataWriteOperation(this);
        }

        protected static class ClearStoryDataWriteOperation extends
                FileBasedWriteOperation<List<String>> {
            // expire in three hours
            private static final long EXPIRATION_HOURS_IN_SECONDS = 3 * 3600;
            private static final long serialVersionUID = 0;
            private static final Logger LOG =
                    LoggerFactory.getLogger(ClearStoryDataWriteOperation.class);

            public ClearStoryDataWriteOperation(ClearStoryDataSink sink) {
                super(sink);
            }

            @Override
            public FileBasedWriter<List<String>> createWriter(PipelineOptions options)
                    throws Exception {
                return new ClearStoryDataWriter(this);
            }

            @Override
            public void finalize(Iterable<FileResult> writerResults, PipelineOptions options)
                    throws Exception {
                LOG.debug("====> CsdWriteOperation.finalize()");
                // We don't use FileBasedSink's finalize(), instead we plug in ourown.
                // super.finalize(writerResults, options);

                csdFinalize(writerResults, options);
            }

            @Override
            public ClearStoryDataSink getSink() {
                return (ClearStoryDataSink) this.sink;
            }


            /**
             * Csd finalization includes split merging, url signing and integration with Csd
             * upload API.
             *
             * @param writerResults the result of the writes
             * @param options       for retrieving pipeline options
             * @throws Exception if error happens
             */
            public void csdFinalize(Iterable<FileResult> writerResults, PipelineOptions options)
                    throws Exception {
                String destination = this.getSink()
                        .sinkInfo
                        .getCsdSinkTop()
                        + "." + ClearStoryDataSink.extension;

                // figure out the size
                List<String> fileNames = new ArrayList<String>();

                // prepare header
                GcsFileUtil.writeHeader(destination,
                        this.getSink().sinkInfo,
                        options);

                for (FileResult fr : writerResults) {
                    fileNames.add(fr.getFilename());
                }

                GcsFileUtil.compose(GCS_FILE_COMPOE_BATCH_SIZE, fileNames, destination, options);

                // clean up the temporary Gcs files
                removeTemporaryFiles(options);

                // getting a signed url for the bucket/object
                String signedUrl = sign("GET", destination);
                LOG.debug("===> signedUrl {}", signedUrl);

                DataSet dataSet = invokeCsdUploadApi(
                        signedUrl);

                // only writes out the data set id in CREATE
                if (this.getSink().sinkInfo.getMode() == CREATE) {
                    GcsFileUtil.writeDataSetId(
                            dataSet.getId(),
                            this.getSink().sinkInfo,
                            options
                    );
                }
            }

            DataSet invokeCsdUploadApi(String signUrl) throws Exception {
                long start = System.currentTimeMillis();
                try {
                    ClearStoryDataClient csdClient = new ClearStoryDataClient(
                            this.getSink().sinkInfo.getEndPoint(),
                            this.getSink().sinkInfo.getApiToken(),
                            this.getSink().sinkInfo.getSslVerification());

                    DataSet dataSet = null;
                    Mode mode = getSink().sinkInfo.getMode();
                    switch (mode) {
                        case CREATE:
                            dataSet = csdClient.createDataSet(
                                    this.getSink().sinkInfo.getDataSetName(),
                                    new GenericUrl(signUrl));
                            LOG.debug("===> DataSet created, id: {}", dataSet.getId());
                            LOG.debug("===> DataSet created, name: {}", dataSet.getName());
                            break;

                        case APPEND:
                            dataSet = csdClient.appendDataSet(
                                    this.getSink().sinkInfo.getDataSetId(),
                                    new GenericUrl(signUrl));
                            LOG.debug("===> DataSet appended, id: {}", dataSet.getId());
                            break;
                        case REPLACE:
                            dataSet = csdClient.replaceDataSet(
                                    this.getSink().sinkInfo.getDataSetId(),
                                    new GenericUrl(signUrl));
                            LOG.debug("===> DataSet replaced, id: {}", dataSet.getId());
                            break;
                        default:
                            LOG.error("===> unknown data set opertion {}", mode);

                    }
                    return dataSet;

                } finally {
                    LOG.debug("===> {}, mode : {}, duration: {} ms",
                            "invokeCsdUploadApi()",
                            this.getSink().sinkInfo.getMode(),
                            System.currentTimeMillis() - start);
                }
            }

            private String sign(String verb,
                                String destination) throws Exception {
                final GcsPath destPath = GcsPath.fromUri(destination);
                long expiration = System.currentTimeMillis() / 1000 + EXPIRATION_HOURS_IN_SECONDS;

                String signedUrl = GcsUrlSigningUtil.getSigningUrl(
                        verb,
                        destPath.getBucket(),
                        destPath.getObject(),
                        this.getSink().sinkInfo.getServiceAccountEmail(),
                        this.getSink().sinkInfo.getKeyStore(),
                        expiration
                );
                return signedUrl;
            }

        }


        static class ClearStoryDataWriter extends FileBasedWriter<List<String>> {
            private static final Logger LOG = LoggerFactory.getLogger(ClearStoryDataWriter.class);
            private OutputStream os = null;

            public ClearStoryDataWriter(ClearStoryDataWriteOperation writeOperation) {
                super(writeOperation);
            }

            @Override
            protected void prepareWrite(WritableByteChannel channel) throws Exception {
                os = Channels.newOutputStream(channel);
            }

            @Override
            public void write(List<String> value) throws Exception {
                StringUtf8Coder.of().encode(
                        (this.getWriteOperation().getSink())
                                .format.getEncoder()
                                .encode(value),
                        os,
                        Coder.Context.OUTER);
            }

            @Override
            public ClearStoryDataWriteOperation getWriteOperation() {
                return (ClearStoryDataWriteOperation) super.getWriteOperation();
            }
        }
    }

    public static class Read {

        public static ClearStoryDataSource withSourceInfo(SourceInfo sourceInfo) {
            return ClearStoryDataSource.withSourceInfo(sourceInfo);
        }

    }

    public static class ClearStoryDataSource extends Source<List<String>> {
        private static final Logger LOG = LoggerFactory.getLogger(ClearStoryDataSource.class);

        private static final long serialVersionUID = 0;
        private final SourceInfo sourceInfo;

        private final Coder<List<String>> coder = ListCoder.of(StringUtf8Coder.of());
        private final Format<List<String>> format = Format.ofCsv();

        public ClearStoryDataSource() {
            this(null);
            ListCoder<String> listCoder = ListCoder.of(StringUtf8Coder.of());
        }

        public ClearStoryDataSource(SourceInfo sourceInfo) {
            this.sourceInfo = sourceInfo;
        }

        public static ClearStoryDataSource withSourceInfo(SourceInfo sourceInfo) {
            return new ClearStoryDataSource(sourceInfo);
        }

        @Override
        public void validate() {
            Preconditions.checkNotNull(sourceInfo, MISSING_SOURCE_INFO);
            Preconditions.checkNotNull(sourceInfo.getApiToken(), MISSING_API_TOEKN);
            Preconditions.checkNotNull(sourceInfo.getDataSetId(), MISSING_DATASET_ID);
            Preconditions.checkNotNull(sourceInfo.getEndPoint(), MISSING_API_TOEKN_CSD_ENDPOINT);

        }

        @Override
        public List<? extends Source<List<String>>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                     PipelineOptions options)
                throws Exception {
            return Arrays.asList(this);
        }

        @Override
        public Coder<List<String>> getDefaultOutputCoder() {
            return coder;
        }

        @Override
        public Reader<List<String>> createReader(
                PipelineOptions options, ExecutionContext executionContext) throws
                IOException {
            return new ClearStoryDataReader(this);
        }

        public static class ClearStoryDataReader implements Reader<List<String>> {
            private static final long serialVersionUID = 0;
            private static final Logger LOG = LoggerFactory.getLogger(ClearStoryDataReader.class);

            private Format.RowReader<List<String>> rowReader;
            private List<String> current;
            private final ClearStoryDataSource source;

            public ClearStoryDataReader(ClearStoryDataSource source) {
                this.source = source;
            }

            /**
             * Initializes the reader and advances the reader to the first record.
             *
             * @return {@code true} if a record was read, {@code false} if we're at the end of
             * input.
             * @throws IOException if error happens
             */
            @Override
            public boolean start() throws IOException {
                InputStream is = null;
                long start = System.currentTimeMillis();
                try {
                    ClearStoryDataClient csdClient = new ClearStoryDataClient(
                            this.source.sourceInfo.getEndPoint(),
                            this.source.sourceInfo.getApiToken(),
                            this.source.sourceInfo.getSslVerification());

                    is = csdClient.queryDataSetData(
                            this.source.sourceInfo.getDataSetId());
                    rowReader = this.getCurrentSource().format.createReader(is);

                    boolean hasNext = rowReader.hasNext();

                    if (hasNext) {
                        current = rowReader.next();
                    }
                    return hasNext;
                } catch (Exception e) {
                    LOG.error("Failed to start", e);
                    try {
                        if (is != null) {
                            is.close();
                        }
                    } catch (IOException ioe) {
                        LOG.warn("Failed to close InputStream", ioe);
                    }
                    throw new IOException("Error in reading data set, id "
                            + this.source.sourceInfo.getDataSetId(), e);
                } finally {
                    LOG.debug("===> {}, duration: {} ms",
                            "csdClient.queryDataSetData()",
                            System.currentTimeMillis() - start);

                }
            }

            @Override
            public boolean advance() throws IOException {
                boolean hasNext = rowReader.hasNext();
                if (hasNext) {
                    current = rowReader.next();
                }
                return hasNext;
            }

            @Override
            public List<String> getCurrent() throws NoSuchElementException {
                return current;
            }

            @Override
            public Instant getCurrentTimestamp() throws NoSuchElementException {
                return BoundedWindow.TIMESTAMP_MIN_VALUE;
            }

            @Override
            public void close() throws IOException {
                if (rowReader != null) {
                    rowReader.close();
                } else {
                    LOG.error("ClearStoryDataReader is not initialized properly!!");
                }
            }

            @Override
            public ClearStoryDataSource getCurrentSource() {
                return source;
            }

        }
    }

}
