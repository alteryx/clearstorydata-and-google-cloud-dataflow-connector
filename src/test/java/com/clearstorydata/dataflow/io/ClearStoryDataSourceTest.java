/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.io;

import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.common.io.CharStreams;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.clearstorydata.dataflow.io.ClearStoryDataIO.ClearStoryDataSource;
import static com.clearstorydata.dataflow.io.ClearStoryDataIO.Read;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;

import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;

/**
 * Created by ianlcsd on 6/24/15.
 */
public class ClearStoryDataSourceTest {
    private static final String API_TOKEN = "test_api_token_123456";
    private static final String DATA_SET_ID = "f752f80d-e096-4521-8600-17829df13afa";
    private static final String URL_PATTERN = "/v1/data_sets/";

    private static Random random = new Random();
    private static final int PORT = random.nextInt(1000) + 1000;

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule(PORT);

    //Work around the limitation that JUnit 4.11 prohibits @Rule on static members
    @Rule
    public WireMockClassRule instanceRule = wireMockRule;

    private String loadResponse() throws Exception {
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("DataSetReadResponse.csv");
        String mockPayload = CharStreams.toString(
                new InputStreamReader(inputStream, "UTF-8"));
        inputStream.close();
        return mockPayload;
    }

    @Test
    public void testRead() throws Exception {
        String mockPayload = loadResponse();

        stubFor(post(urlPathEqualTo(URL_PATTERN + DATA_SET_ID + "/query"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(mockPayload)));


        SourceInfo sourceInfo = SourceInfo.builder()
                .setApiToken(API_TOKEN)
                .setDataSetId(DATA_SET_ID)
                .setEndPoint("http://localhost:" + PORT)
                .setSslVerification(false)
                .build();

        ClearStoryDataSource csdSource = Read.withSourceInfo(sourceInfo);

        Source.Reader<List<String>> reader =  csdSource.createReader(null, null);
        boolean next = reader.start();
        List<List<String>> rows = new ArrayList<>();
        while (next) {
            List<String> row = reader.getCurrent();
            Assert.assertEquals(8, row.size());
            rows.add(row);
            next = reader.advance();
        }

        // assert the row numer
        Assert.assertEquals(10, rows.size());
        verify(postRequestedFor(
                        // ensuring hitting right endpoint
                        urlMatching(URL_PATTERN + DATA_SET_ID + "/query"))
                        // ensure token is passed in
                        .withHeader("x-apitokenid", containing(API_TOKEN))
        );
    }
}
