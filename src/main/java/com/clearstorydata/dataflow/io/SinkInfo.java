/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.io;

import com.clearstorydata.resource.dataset.Schema;

import java.io.Serializable;

/**
 * Created by ianlcsd on 6/23/15.
 */
public class SinkInfo implements Serializable {
    private static final long serialVersionUID = 0;

    public static class Builder {
        private String endPoint;
        private String intermediateLocation;
        private String apiToken;
        private String dataSetName;
        private String dataSetId;
        private Mode mode;
        private Schema schema;
        private byte[] keyStore;
        private String serviceAccountEmail;
        private boolean sslVerification = true;

        public Builder setSslVerification(boolean sslVerification) {
            this.sslVerification = sslVerification;
            return this;
        }

        public Builder setEndPoint(String endPoint) {
            this.endPoint = endPoint;
            return this;
        }

        public Builder setIntermediateLocation(String intermediateLocation) {
            this.intermediateLocation = intermediateLocation;
            return this;
        }

        public Builder setApiToken(String apiToken) {
            this.apiToken = apiToken;
            return this;
        }

        public Builder setDataSetName(String dataSetName) {
            this.dataSetName = dataSetName;
            return this;
        }

        public Builder setDataSetId(String dataSetId) {
            this.dataSetId = dataSetId;
            return this;
        }

        public Builder setMode(Mode mode) {
            this.mode = mode;
            return this;
        }

        public Builder setSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setKeyStore(byte[] keyStore) {
            this.keyStore = keyStore;
            return this;
        }

        public Builder setServiceAccountEmail(String serviceAccountEmail) {
            this.serviceAccountEmail = serviceAccountEmail;
            return this;
        }

        /**
         * @return a Sink info built from the staged info.
         */
        public SinkInfo build() {
            return new SinkInfo(endPoint, intermediateLocation, apiToken,
                    dataSetName, dataSetId, mode, schema, keyStore,
                    serviceAccountEmail, sslVerification);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum Mode {CREATE, APPEND, REPLACE}

    private final String endPoint;
    private final String intermediateLocation;
    private final String apiToken;
    private final String dataSetName;
    private final String dataSetId;
    private final Mode mode;
    private final Schema schema;
    private final byte[] keyStore;
    private final String serviceAccountEmail;
    private final boolean sslVerification;

    public String getEndPoint() {
        return endPoint;
    }

    public String getIntermediateLocation() {
        return intermediateLocation;
    }

    public String getApiToken() {
        return apiToken;
    }

    public String getDataSetName() {
        return dataSetName;
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public Mode getMode() {
        return mode;
    }

    public Schema getSchema() {
        return schema;
    }

    public byte[] getKeyStore() {
        return keyStore;
    }

    public String getServiceAccountEmail() {
        return serviceAccountEmail;
    }

    public boolean getSslVerification() {
        return sslVerification;
    }

    /**
     * This api is a helper that determines Gcs location to be used by CsdSink.
     * @return the Gcs location for the sink to use.
     */
    public String getCsdSinkTop() {
        return intermediateLocation + "/"
                + ( mode == Mode.CREATE ? dataSetName : dataSetId);
    }

    /**
     * SinkInfo is the data holder for all the necessary information when pushing data to
     * clearstory.
     *
     * @param endPoint             clearstory data service endpoint, for instance, https://dev
     *                             .clearstorydatainc.com
     * @param intermediateLocation the "gs://" location for csd to stage intermediate files
     * @param apiToken             clearstory data authentication token
     * @param dataSetName          data set name, needed when create data set.
     * @param dataSetId            data set id, needed when append/replace data set.
     * @param mode                 data set operation: CREATE/APPEND/REPLACE
     * @param schema               physical schema for the data set.
     * @param keyStore             google cloud pksc12 keystore in bytes
     * @param serviceAccountEmail  google cloud service account email
     * @param sslVerification      indicates whether to perform ssl verification
     */
    public SinkInfo(String endPoint, String intermediateLocation, String apiToken, String
            dataSetName, String dataSetId, Mode mode, Schema schema, byte[] keyStore,
                    String serviceAccountEmail, boolean sslVerification) {
        this.endPoint = endPoint;
        this.intermediateLocation = intermediateLocation;
        this.apiToken = apiToken;
        this.dataSetName = dataSetName;
        this.dataSetId = dataSetId;
        this.mode = mode;
        this.schema = schema;
        this.keyStore = keyStore;
        this.serviceAccountEmail = serviceAccountEmail;
        this.sslVerification = sslVerification;
    }
}
