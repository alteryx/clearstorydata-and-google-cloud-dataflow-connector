clearstorydata-dataflow
=======================
clearstorydata-dataflow is a ClearStory Data client library for Google DataFlow. 
It enables first phase integration, that allows dataflow pipelines to interact with ClearStory Data as external Source and Sink.


# Get Started
*   Google Cloud Platform Setup

  1. Start by following the general Cloud Dataflow
  [Getting Started](https://cloud.google.com/dataflow/getting-started) instructions.

  2. Installed and authenticated Google Cloud SDK. 

  3. You should have a Google Cloud Platform project that has a Cloud Dataflow API enabled,

  4. You should have a Google Cloud Storage bucket that will serve as a staging location.

*   To work with ClearStory Data access model, make sure the following is ready:

  1. A ClearStory Data API token for authentication.

  2. ClearStory Data public DataSet/DataSource APIs are enabled for the organization to which the user belongs. 

# Build

```bash
mvn clean install
```

# Unit Test Only

```bash
mvn clean test -P dev
```

# ClearStory Data Client

## CsdSink

CSD is acting as an external Data Sink to DataFlow, allowing pipelines creating Data Set
 in CSD.


### Data Set Creation Example
In the following example, we retrieve data from a BigQuery table, 
"clouddataflow-readonly:samples.weather_stations", and create 
a data set in ClearStory Data environment.

 
#### Options
* --project : The project ID for your Google Cloud Project.  
* --stagingLocation : Google Cloud Storage path for staging local files. Must be a valid Cloud Storage URL, beginning with gs://. 
* --runner : The PipelineRunner to use. This field allows you to determine the PipelineRunner at 
runtime. Available options are DataflowPipelineRunner, DirectPipelineRunner 
 and BlockingDataflowPipelineRunner. 
 See [details](https://cloud.google.com/dataflow/pipelines/executing-your-pipeline)
* --serviceAccountEmail : the email address of your OAuth Service Account.
* --privateKeyPath : the path to the Pkcs12 file, from you OAuth Service Account, on the local 
machine where you submit the job.
* --csdEndPoint : The public API end point url of ClearStory Data. 
* --csdIntermediateLocation :  Google Cloud Storage path for staging the intermediate file.
* --csdCurMode : The Data Set Operation mode, CREATE/UPDATE/REPLACE.  
* --csdDataSetName : The logical name for the data set to be created in ClearStory Data. This is 
required when --csdCurMode=CREATE is used.  
* --csdDataSetId : The id of the data set to be appended or replaced in ClearStory Data. This is 
required when --csdCurMode=APPEND or --csdCurMode=REPLACE is used.  
* --csdApiToken : The api token provisioned from ClearstoryData.
* --sslVerification :  A flag allowing to switch off ssl verification, used on non-production integration testing.
    
```bash
export PROJECT_ID="...."
export CSD_END_POINT="...."
export CSD_API_TOKEN="...."
export CSD_INTERMEDIATE_LOCATION="...."
export RUNNER="DataflowPipelineRunner"
export SERVICE_ACCOUNT_EMAIL="...."
export PRIVATE_KEY_PATH="...."
export CSD_DATASET_NAME="my_csd_test_sink_data_set"
export STAGING="...."
export MODE="CREATE"
export SSL_VERIFICATION="false"

mvn exec:java -Dexec.mainClass=com.clearstorydata.dataflow.sample.ClearStoryDataSinkDemo \
-Dexec.args="\
  --project=${PROJECT_ID} \
  --csdEndPoint=${CSD_ENDPOINT} \
  --csdIntermediateLocation=${CSD_INTERMEDIATE_LOCATION} \
  --csdDataSetName=${CSD_DATASET_NAME} \
  --csdCurMode=${MODE} \
  --csdApiToken=${CSD_API_TOKEN} \
  --privateKeyPath=${PRIVATE_KEY_PATH} \
  --defaultWorkerLogLevel=DEBUG \
  --stagingLocation=${STAGING} \
  --runner=${RUNNER} \
  --serviceAccountEmail=${SERVICE_ACCOUNT_EMAIL} \
  --sslVerification=${SSL_VERIFICATION} \
"
```
### Data Set Append/Replace Example
Other than Data Set Create, ClearStory Data allows Append and Replace operation. The way running
 Data Set Append and Replace is similar to Create,
except that Data Set Id and operation Mode needs to be specified.  

The following example is demostrating Data Set Replace. 
In similar way, Data Set Append can be done by using option "--csdCurMode=APPEND"

```bash
export PROJECT_ID="...."
export CSD_END_POINT="...."
export CSD_API_TOKEN="...."
export CSD_INTERMEDIATE_LOCATION="...."
export RUNNER="DataflowPipelineRunner"
export SERVICE_ACCOUNT_EMAIL="...."
export PRIVATE_KEY_PATH="...."
export STAGING="...."
export MODE="REPLACE"
export CSD_DATASET_ID=<your_data_set_id>
export SSL_VERIFICATION="false"

mvn exec:java -Dexec.mainClass=com.clearstorydata.dataflow.sample.ClearstoryDataSinkDemo \
-Dexec.args="\
  --project=${PROJECT_ID} \
  --csdEndPoint=${CSD_ENDPOINT} \
  --csdIntermediateLocation=${CSD_INTERMEDIATE_LOCATION} \
  --csdDataSetId=${CSD_DATASET_ID} \
  --csdCurMode=${MODE} \
  --csdApiToken=${CSD_API_TOKEN} \
  --privateKeyPath=${PRIVATE_KEY_PATH} \
  --defaultWorkerLogLevel=DEBUG \
  --stagingLocation=${STAGING} \
  --runner=${RUNNER} \
  --serviceAccountEmail=${SERVICE_ACCOUNT_EMAIL} \
  --sslVerification=${SSL_VERIFICATION} \
"
```


## CsdSource

CSD is acting as an external Data Source to DataFlow, allowing pipelines consuming Data Set from CSD.


### Example
In the following example we retrieve data from ClearStory Data dev environment and write it to a Gcs file.

#### Options
* --project : The project ID for your Google Cloud Project.  
* --stagingLocation : Google Cloud Storage path for staging local files. Must be a valid Cloud Storage URL, beginning with gs://. 
* --runner : The PipelineRunner to use. This field allows you to determine the PipelineRunner at 
runtime. Available options are DataflowPipelineRunner, DirectPipelineRunner 
 and BlockingDataflowPipelineRunner. See 
[details](https://cloud.google.com/dataflow/pipelines/executing-your-pipeline)
* --csdEndPoint : The Clearstory Data public API end point url. 
* --csdDataSet : The logical name for the data set created in ClearStory Data 
* --csdApiToken : The api token provisioned from ClearstoryData.  
* --sslVerification :  A flag allowing to switch off ssl verification, used on non-production integration testing.

```bash
export PROJECT_ID="...."
export CSD_END_POINT="...."
export CSD_DATASET_ID="...."
export CSD_API_TOKEN="...."
export STAGING="...."
export OUTPUT="...."
export RUNNER=DataflowPipelineRunner
export SSL_VERIFICATION="false"

mvn exec:java -Dexec.mainClass=com.clearstorydata.dataflow.sample.ClearStoryDataSourceDemo \
-Dexec.args="\
  --project=${PROJECT_ID} \
  --csdEndPoint=${CSD_END_POINT} \
  --csdDataSetId=${CSD_DATASET_ID} \
  --csdApiToken=${CSD_API_TOKEN} \
  --defaultWorkerLogLevel=DEBUG \
  --stagingLocation=${STAGING} \
  --output=${OUTPUT} \
  --runner=${RUNNER} \
  --sslVerification=${SSL_VERIFICATION} \
"
```
