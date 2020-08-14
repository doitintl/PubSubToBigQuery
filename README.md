# PubSub to BigQuery

```bash
export GOOGLE_APPLICATION_CREDENTIALS=<path to json service account key>

export BUCKET_NAME=<your bucket name here>

export PROJECT_ID=<your project name here>

# Build
./gradlew clean shadowJar

# Ordered pipeline
java -jar build/libs/pubsubtobigquery-1.0.0.jar \
 --runner=DataflowRunner --gcpTempLocation="${BUCKET_NAME}"/ordered \
 --workerZone=europe-west1-b --project="${PROJECTID}" \
 --inputSubscription=projects/"${PROJECTID}"/subscriptions/ordered \
 --outputTableSpec="${PROJECTID}":orderedtest.ordered

# Unordered pipeline
java -jar build/libs/pubsubtobigquery-1.0.0.jar \
 --runner=DataflowRunner --gcpTempLocation="${BUCKET_NAME}"/unordered \
 --workerZone=europe-west1-b --project="${PROJECTID}" \
 --inputSubscription=projects/"${PROJECTID}"/subscriptions/unordered \
 --outputTableSpec="${PROJECTID}":orderedtest.unordered
```
