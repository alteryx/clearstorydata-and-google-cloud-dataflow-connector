/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.io;

import com.clearstorydata.dataflow.io.ClearStoryDataIO.ClearStoryDataSink;
import com.clearstorydata.dataflow.io.ClearStoryDataIO.ClearStoryDataSink.ClearStoryDataWriteOperation;
import com.clearstorydata.resource.dataset.Schema;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.google.common.io.CharStreams;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;

/**
 * Created by ianlcsd on 6/24/15.
 */
public class ClearStoryDataSinkTest {
    private static final int PORT = 12345;
    private static final String URL_PATTERN = "/v1/data_sets/";
    private static final String DOWNLOAD_URL = "http://someurlhere";
    private static final String DATA_SET_NAME = "test_data_set_name_123456";
    private static final String DATA_SET_ID = "f752f80d-e096-4521-8600-17829df13afa";
    private static final String COL1_NAME = "column1";
    private static final String API_TOKEN = "test_api_token_123456";

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule(PORT);

    //Work around the limitation that JUnit 4.11 prohibits @Rule on static members
    @Rule
    public WireMockClassRule instanceRule = wireMockRule;

    private String loadResponse() throws Exception {
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("DataSetCreateResponse.json");
        String mockPayload = CharStreams.toString(
                new InputStreamReader(inputStream, "UTF-8"));
        inputStream.close();
        return mockPayload;
    }

    @Test
    public void testCreate() throws Exception {
        String mockPayload = loadResponse();

        stubFor(post(urlPathEqualTo(URL_PATTERN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(mockPayload)));

        SinkInfo sinkInfo = SinkInfo.builder()
                .setApiToken(API_TOKEN)
                .setDataSetName(DATA_SET_NAME)
                .setEndPoint("http://localhost:" + PORT)
                .setSchema(new Schema().addColumn(COL1_NAME, Schema.Column.Type.CATEGORY))
                .setMode(SinkInfo.Mode.valueOf("CREATE"))
                .build();

        ClearStoryDataSink csdSink = ClearStoryDataIO.Write.withSinkInfo(sinkInfo);

        ClearStoryDataWriteOperation csdWriteOperation = csdSink
                .createWriteOperation(null);
        csdWriteOperation.invokeCsdUploadApi(DOWNLOAD_URL);

        verify(postRequestedFor(
                        // ensuring hitting right endpoint
                        urlMatching(URL_PATTERN))
                        // ensure schema is passed in
                        //.withRequestBody(matching(".*schema.*" + COL1_NAME + ".*"))
                        //.withRequestBody(matching(".*schema.*" + Schema.Column.Type.CATEGORY
                        //        + ".*"))
                        // ensure data set name is passed in
                        .withRequestBody(matching(".*name.*" + DATA_SET_NAME + ".*"))
                        // ensure sourceUrl is passed in
                        .withRequestBody(matching(".*sourceUrl.*"
                                + java.net.URLEncoder.encode(DOWNLOAD_URL, "ISO-8859-1") + ".*"))
                        // ensure api token is passed in is passed in
                        .withHeader("x-apitokenid", containing(API_TOKEN))

        );
    }

    @Test
    public void testAppend() throws Exception {
        String mockPayload = loadResponse();

        stubFor(put(urlPathEqualTo(URL_PATTERN + DATA_SET_ID + "/append"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(mockPayload)));

        SinkInfo sinkInfo = SinkInfo.builder()
                .setApiToken(API_TOKEN)
                .setDataSetId(DATA_SET_ID)
                .setEndPoint("http://localhost:" + PORT)
                .setSslVerification(false)
                .setMode(SinkInfo.Mode.valueOf("APPEND"))
                .build();

        ClearStoryDataSink csdSink = ClearStoryDataIO.Write.withSinkInfo(sinkInfo);

        ClearStoryDataWriteOperation csdWriteOperation = csdSink
                .createWriteOperation(null);
        csdWriteOperation.invokeCsdUploadApi(DOWNLOAD_URL);

        verify(putRequestedFor(
                        // ensuring hitting right endpoint
                        urlMatching(URL_PATTERN + DATA_SET_ID + "/append"))
                        // ensure sourceUrl is passed in
                        .withRequestBody(matching(".*sourceUrl.*"
                                + java.net.URLEncoder.encode(DOWNLOAD_URL, "ISO-8859-1") + ".*"))
                        // ensure api token is passed in is passed in
                        .withHeader("x-apitokenid", containing(API_TOKEN))
        );
    }

    @Test
    public void testReplace() throws Exception {
        String mockPayload = loadResponse();

        stubFor(put(urlPathEqualTo(URL_PATTERN + DATA_SET_ID + "/replace"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(mockPayload)));

        SinkInfo sinkInfo = SinkInfo.builder()
                .setApiToken(API_TOKEN)
                .setDataSetId(DATA_SET_ID)
                .setEndPoint("http://localhost:" + PORT)
                .setMode(SinkInfo.Mode.valueOf("REPLACE"))
                .build();

        ClearStoryDataSink csdSink = ClearStoryDataIO.Write.withSinkInfo(sinkInfo);

        ClearStoryDataWriteOperation csdWriteOperation = csdSink
                .createWriteOperation(null);
        csdWriteOperation.invokeCsdUploadApi(DOWNLOAD_URL);

        verify(putRequestedFor(
                        // ensuring hitting right endpoint
                        urlMatching(URL_PATTERN + DATA_SET_ID + "/replace"))
                        // ensure sourceUrl is passed in
                        .withRequestBody(matching(".*sourceUrl.*"
                                + java.net.URLEncoder.encode(DOWNLOAD_URL, "ISO-8859-1") + ".*"))
                                // ensure api token is passed in is passed in
                        .withHeader("x-apitokenid", containing(API_TOKEN))
        );
    }

}
