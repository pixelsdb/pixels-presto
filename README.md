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

Ensure that Pixels and other prerequisites are installed following the instructions
[HERE](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md).
Instead of installing Trino as the query engine, here we install Presto.

### Install Presto

Download and install Presto-0.279 following the instructions in [Presto Docs](https://prestodb.io/docs/0.279/installation/deployment.html).

Here, we install Presto to `~/opt/presto-server-0.279` and create a link for it:
```bash
ln -s presto-server-0.279 presto-server
```
Then download [presto-cli](https://prestodb.io/docs/0.279/installation/cli.html) into `~/opt/presto-server/bin/`
and give executable permission to it.
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
clean.local.result=true
output.scheme=s3
output.folder=output-folder-dummy
output.endpoint=output-endpoint-dummy
output.access.key=lambda
output.secret.key=password
```
`pixels.config` is used to specify the config file for Pixels, and has a higher priority than the config file under `PIXELS_HOME`.
**Note** that `etc/catalog/pixels.proterties` under Presto's home is different from `PIXELS_HOME/pixels.properties`.
The other properties are related to serverless execution.
In Presto, Pixels can push down table scan (i.e., projection and selection) into AWS lambda. This is similar to Redshift Spectrum.
This feature can be turned on by setting `lambda.enabled` to `true`, `output.scheme` to the storage scheme of the intermediate files (e.g. s3),
`output.folder` to the directory of the intermediate files, `output.endpoint` to the endpoint of the intermediate storage,
and `output.access/secret.key` to the access/secret key of the intermediate storage.

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
