# pixels-presto
This is the Pixels integration for PrestoDB.

## Compatibility
Pixels integration (connector & event listener) is currently compatible with Presto 0.215. Other Presto versions that are compatible
with the Connector SPI in Presto 0.215 should also work well with Pixels.

## Build

This project can be opened as a maven project in Intellij and built using maven.

**Note** that Presto 0.215 requires Java 8, thus this project must be build by Jdk 8.

However, [Pixels](https://github.com/pixelsdb/pixels) is the parent of this project, 
therefore use `mvn install` to install Pixels modules into your local maven repository,
before building this project.

## Use Pixels in Presto

Ensure that Pixels and other prerequisites are installed following the instructions
[here](https://github.com/pixelsdb/pixels#installation-in-aws).

### Install Pixels Connector
There are two important directories in the home of presto-server: `etc` and `plugin`.
To install Pixels connector, decompress `pixels-presto-connector-*.zip` into the `plugin` directory.
The `etc` directory contains the configuration files of Presto.
In addition to the configurations mentioned in the official docs, 
create the catalog config file named `pixels.properties` for Pixels in the `etc/catalog` directory, with the following content:
```properties
connector.name=pixels
pixels.home=/home/ubuntu/opt/pixels/
```
`pixels.home` should be the same as `PIXELS_HOME`.
**Note** that this `pixels.properties` is in the `etc/catalog` directory of Presto's home, and is different from `PIXELS_HOME/pixels.properties`.

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

Follow the instructions [here](https://github.com/pixelsdb/pixels#start-pixels) to start Pixels and use it in Presto.
Then you can [evaluate the queries in TPC-H](https://github.com/pixelsdb/pixels#tpc-h-evaluation).
