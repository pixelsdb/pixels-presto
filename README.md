# pixels-presto
This is the Pixels integration for PrestoDB.

## Compatibility
Pixels integration (connector & event listener) is currently compatible with Presto 0.279. Other Presto versions that are compatible
with the Connector SPI in Presto 0.279 should also work well with Pixels.

## Build

This project can be opened as a maven project in Intellij and built using maven.

**Note** that Presto 0.279 requires Java 8, thus this project must be built by JDK 8.

However, [Pixels](https://github.com/pixelsdb/pixels) is the parent of this project, 
therefore use `mvn install` to install Pixels modules into your local maven repository before building this project.

## Use Pixels in Presto

Follow the instructions in
[Pixels Installation](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md) to install Pixels and other components except Trino.
Instead of using Trino as the query engine, here we install Presto and use it to query Pixels.
Ensure Java 8 is in use as it is required by Presto 0.279.

### Install Presto

Download and install Presto-0.279 following the instructions in [Presto Docs](https://prestodb.io/docs/0.279/installation/deployment.html).

Here, we install Presto to `~/opt/presto-server-0.279` and create a link for it:
```bash
cd ~/opt; ln -s presto-server-0.279 presto-server
```
Then download [presto-cli](https://prestodb.io/docs/0.279/installation/cli.html) into `~/opt/presto-server/bin/`
and give the executable permission to it.
Some scripts in Presto may require python:
```bash
sudo apt-get install python
```

### Install Pixels Connector
There are two important directories in the home of presto-server: `etc` and `plugin`.
To install Pixels connector, decompress `pixels-presto-connector-*.zip` into the `plugin` directory.
The `etc` directory contains the configuration files of Presto.
In addition to the configurations mentioned in the official docs, 
create the catalog config file named `pixels.properties` for Pixels in the `etc/catalog` directory, with the following content:
```properties
connector.name=pixels
pixels.config=/home/ubuntu/opt/pixels/pixels.properties

# serverless config
lambda.enabled=false
local.scan.concurrency=0
clean.intermediate.result=true
```
`pixels.config` is used to specify the config file for Pixels, and has a higher priority than the config file under `PIXELS_HOME`.
**Note** that `etc/catalog/pixels.proterties` under Presto's home is different from `PIXELS_HOME/pixels.properties`.
The other properties are related to serverless execution.
In Presto, Pixels can push down table scan (i.e., projection and selection) into serverless computing services such as AWS lambda. 
This feature is similar to Redshift Spectrum, and it is a restricted version of 
[`Pixels Turbo`](https://github.com/pixelsdb/pixels/tree/master/pixels-turbo).
We call it `Pixels Turbo Lite`.
It can be turned on by setting `lambda.enabled` to `true`.

If `Pixels Turbo Lite` is enabled, we also need to set the following settings in `PIXELS_HOME/pixels.properties`:
```properties
executor.input.storage.scheme=s3
executor.output.storage.scheme=output-storage-scheme-dummy
executor.output.folder=output-folder-dummy
```
Those storage schemes and folders are used to access the input data
(the data of the base tables defined by `CREATE TABLE` statements) and the output data 
(the results of the table scans executed in the serverless workers), respectively.
Ensure they are valid so that the serverless workers can access the corresponding storage systems.
Especially, the `executor.input.storage.scheme` must be consistent with the storage scheme of the base
tables. This is checked during query-planning for Pixels Turbo.
In addition, the `executor.output.folder` is the base path where the scan output is stored. 
It also needs to be valid and accessible for the serverless workers.
If other storage scheme such as `minio` is used, ensure the related properties also configured in `PIXELS_HOME/pixels.properties`:
```properties
minio.region=eu-central-2
minio.endpoint=http://minio-host-dummy:9000
minio.access.key=minio-access-key-dummy
minio.secret.key=minio-secret-key-dummy
```

### Install Pixels Event Listener*
Pixels event listener is optional. It is used to collect the query completion information for performance evaluations.
To install the event listener, decompress `pixels-presto-listener-*.zip` into the `plugin` directory.

Create the listener config file named `event-listener.properties` in the `etc` directory, with the following content:
```properties
event-listener.name=pixels-event-listener
enabled=true
listened.user.prefix=none
listened.schema=pixels
listened.query.type=SELECT
log.dir=/home/ubuntu/opt/pixels/listener/
```
`log-dir` should point to an existing directory where the listener logs will appear.

### Run Queries

Start Pixels + Presto in the same way as [Starting Pixels + Trino](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md).
The usage is also quite similar as the usage of Pixels + Trino.
Then, you can test the query performance following [TPC-H Evaluation](https://github.com/pixelsdb/pixels/blob/master/docs/TPC-H.md).
