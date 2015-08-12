/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.io;

import java.io.Serializable;

/**
 * Created by ianlcsd on 6/23/15.
 */
public class SourceInfo implements Serializable {
    private static final long serialVersionUID = 0;

    public static class Builder {
        private String endPoint;
        private String apiToken;
        private String dataSetId;
        private boolean sslVerification = true;


        public Builder setSslVerification(boolean sslVerification) {
            this.sslVerification = sslVerification;
            return this;
        }

        public Builder setEndPoint(String endPoint) {
            this.endPoint = endPoint;
            return this;
        }

        public Builder setApiToken(String apiToken) {
            this.apiToken = apiToken;
            return this;
        }

        public Builder setDataSetId(String dataSetId) {
            this.dataSetId = dataSetId;
            return this;
        }

        public SourceInfo build() {
            return new SourceInfo(endPoint, apiToken, dataSetId, sslVerification);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final String endPoint;
    private final String apiToken;
    private final String dataSetId;
    private final boolean sslVerification;

    public String getEndPoint() {
        return endPoint;
    }

    public String getApiToken() {
        return apiToken;
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public boolean getSslVerification() {
        return sslVerification;
    }

    /**
     * SourceInfo contians all the necessary information for data set to be retrieved from
     * clearstory.
     *
     * @param endPoint        clearstory data service endpoint, for instance, http://dev
     *                        .clearstorydatainc.com
     * @param apiToken        clearstory data authentication token
     * @param dataSetId       data set id, needed when append/replace data set
     * @param sslVerification indicates whether to perform ssl verification
     */
    public SourceInfo(String endPoint, String apiToken, String dataSetId, boolean sslVerification) {
        this.endPoint = endPoint;
        this.apiToken = apiToken;
        this.dataSetId = dataSetId;
        this.sslVerification = sslVerification;
    }
}
