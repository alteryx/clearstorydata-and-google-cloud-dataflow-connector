/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.util;

import com.clearstorydata.dataflow.io.ClearStoryDataIO.ClearStoryDataSink;
import com.clearstorydata.dataflow.io.SinkInfo;
import com.clearstorydata.resource.dataset.Schema;
import com.clearstorydata.resource.dataset.Schema.Column;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.Charsets;
import com.google.api.client.util.Preconditions;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ianlcsd on 6/29/15.
 */
public class GcsFileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(GcsFileUtil.class);

    /**
     * This api copies the Gcs fie to the destination file of different name.
     *
     * @param source      the source to be copied from
     * @param destination the source to be to copied to
     * @param options     needed by Gcs Storage client
     * @throws IOException if any error happens
     */
    public static void copy(String source, String destination,
                            PipelineOptions options) throws Exception {
        LOG.debug("====> Copying {} to ().", source, destination);
        GcsOptions gcsOptions = options.as(GcsOptions.class);
        Storage gcs = Transport.newStorageClient(gcsOptions).build();

        final GcsPath sourcePath = GcsPath.fromUri(source);
        final GcsPath destPath = GcsPath.fromUri(destination);
        LOG.debug("Copying {} to {}", sourcePath, destPath);
        Storage.Objects.Copy copyObject = gcs.objects().copy(sourcePath.getBucket(),
                sourcePath.getObject(), destPath.getBucket(), destPath.getObject(),
                null);
        copyObject.execute();

    }

    static List<List<String>> arrangeBatch(int batchSize, List<String> srcFileNames, String
            destFilename) {
        Preconditions.checkArgument(batchSize > 1);
        Preconditions.checkArgument(destFilename != null);

        List<List<String>> arrangedList = new ArrayList<List<String>>();

        for (int i = 0; i * (batchSize - 1) <= srcFileNames.size() ; i++) {
            if (i * (batchSize - 1) < srcFileNames.size() ) {
                List<String> arranged = new ArrayList<>();
                arranged.add(0, destFilename);
                arranged.addAll(
                        srcFileNames.subList(
                                i * (batchSize - 1),
                                Math.min(
                                        (i + 1) * (batchSize - 1),
                                        srcFileNames.size()
                                )));
                arrangedList.add(arranged);
            }
        }

        return arrangedList;
    }

    /**
     * This api composes the Gcs files to the destination file.
     *
     * @param batchSize    the szie of batch per composition
     * @param srcFileNames the source files to be merged from
     * @param destFilename the source to be to be merged to
     * @param options      needed by Gcs Storage client
     * @throws IOException if any error happens
     */
    public static void compose(int batchSize, List<String> srcFileNames, String destFilename,
                               PipelineOptions options) throws Exception {
        List<List<String>> arrangedBatch = arrangeBatch(batchSize, srcFileNames, destFilename);

        // compose the chunk
        for (List<String> batch : arrangedBatch) {
            GcsFileUtil.compose(
                    batch,
                    destFilename,
                    options);
        }
    }


    /**
     * This api merges the Gcs files to the destination file.
     *
     * @param srcFileNames the source files to be merged from
     * @param destFilename the source to be to be merged to
     * @param options      needed by Gcs Storage client
     * @throws IOException if any error happens
     */
    private static void compose(List<String> srcFileNames, String destFilename,
                               PipelineOptions options) throws Exception {

        List<ComposeRequest.SourceObjects> sourceObjects =
                new ArrayList<>();

        for (String src : srcFileNames) {
            final GcsPath sourcePath = GcsPath.fromUri(src);
            ComposeRequest.SourceObjects sr = new ComposeRequest.SourceObjects();
            LOG.debug("===> composing {}", sourcePath.getObject());
            sourceObjects.add(
                    sr.setName(sourcePath.getObject())
            );
        }

        GcsOptions gcsOptions = options.as(GcsOptions.class);
        Storage gcs = Transport.newStorageClient(gcsOptions).build();

        final GcsPath destPath = GcsPath.fromUri(destFilename);

        Storage.Objects.Compose composeObject = gcs.objects().compose(
                destPath.getBucket(),
                destPath.getObject(),
                new ComposeRequest()
                        .setSourceObjects(sourceObjects)
                        .setDestination(
                                new StorageObject()
                                        .setContentType("text/plain")));
        composeObject.execute();
    }

    /**
     * Write off the header information to a file.
     *
     * @param dest     the destination file
     * @param sinkInfo SinkInfo constructed from Data Set creation process
     * @param options  needed by Gcs Storage client
     * @throws IOException if any error happens with Gcs operation
     */
    public static void writeHeader(String dest,
                                   SinkInfo sinkInfo,
                                   PipelineOptions options) throws IOException {

        Schema schema = sinkInfo.getSchema();
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Column col : schema.getColumns()) {
            if (isFirst) {
                sb.append(col.getName());
                isFirst = false;
            } else {
                sb.append(", " + col.getName());
            }
        }
        sb.append("\n");
        String header = sb.toString();


        InputStreamContent mediaContent = new InputStreamContent("text/plain", new
                ByteArrayInputStream(header.getBytes()));
        mediaContent.setLength(header.getBytes().length);
        GcsOptions gcsOptions = options.as(GcsOptions.class);
        Storage gcs = Transport.newStorageClient(gcsOptions).build();
        GcsPath destPath = GcsPath.fromUri(dest);
        Storage.Objects.Insert insertObject = gcs.objects().insert(
                destPath.getBucket(),
                null,
                mediaContent);
        insertObject.setName(destPath.getObject());
        insertObject.execute();
    }


    /**
     * Since in the normal job running case, the data set is created asynchrously, the user
     * will have to look up the id of the newly created data set, by either peeking into the log or
     * looking at Csd's Data Set page.
     * In order for us to run full Data Set testing flow, CREATE => APPEND => REPLACE,
     * we use this api to write csd data set id into a temp file, and communicating
     * the data set id information to downstream testing logic.
     * the api is only called from {@link ClearStoryDataSink
     * .CsdWriteOperation#csdFinalize}, in the context of data set creation, the {@link SinkInfo
     * #getDataSetName} should be not null.
     * See the the {@link #readDataSetId}
     *
     * @param dataSetId csd data set id
     * @param sinkInfo  SinkInfo constructed from Data Set creation process
     * @param options   needed by Gcs Storage client
     * @throws IOException if any error happens with Gcs operation
     */
    public static void writeDataSetId(String dataSetId,
                                      SinkInfo sinkInfo,
                                      PipelineOptions options) throws IOException {
        Preconditions.checkNotNull(sinkInfo.getDataSetName());

        String destination = sinkInfo.getCsdSinkTop()
                + "/" + sinkInfo.getDataSetName()
                + ".dataSetId";

        GcsOptions gcsOptions = options.as(GcsOptions.class);
        Storage gcs = Transport.newStorageClient(gcsOptions).build();
        GcsPath destPath = GcsPath.fromUri(destination);
        InputStreamContent mediaContent = new InputStreamContent("text/plain", new
                ByteArrayInputStream(dataSetId.getBytes()));
        mediaContent.setLength(dataSetId.getBytes().length);
        Storage.Objects.Insert insertObject = gcs.objects().insert(
                destPath.getBucket(),
                null,
                mediaContent);
        insertObject.setName(destPath.getObject());
        insertObject.execute();
    }

    /**
     * This api reads the data set id from a temporarily staged Gcs file,
     * created by {@link #writeDataSetId} api
     * the api is only called in the context of data set creation, the {@link SinkInfo
     * #getDataSetName} should be not null.
     *
     * @param sinkInfo SinkInfo constructed from Data Set creation process
     * @return csd data set id
     * @throws IOException if any error happens
     */
    public static String readDataSetId(SinkInfo sinkInfo,
                                       PipelineOptions options) throws IOException {
        Preconditions.checkNotNull(sinkInfo.getDataSetName());

        String destination = sinkInfo.getCsdSinkTop()
                + "/" + sinkInfo.getDataSetName()
                + ".dataSetId";


        GcsOptions gcsOptions = options.as(GcsOptions.class);
        Storage gcs = Transport.newStorageClient(gcsOptions).build();
        GcsPath destPath = GcsPath.fromUri(destination);
        Storage.Objects.Get getObject = gcs.objects().get(
                destPath.getBucket(),
                destPath.getObject()
        );


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        getObject.executeMediaAndDownloadTo(out);
        String dataSetid = out.toString(Charsets.UTF_8.name());

        return dataSetid;
    }
}
