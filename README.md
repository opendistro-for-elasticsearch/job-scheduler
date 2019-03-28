# Open Distro for Elasticsearch Job Scheduler

Open Distro for Elasticsearch JobScheduler plugin provides a framework for Elasticsearch plugin
developers to schedule periodical jobs running within Elasticsearch nodes. You can schedule jobs
by specify an interval, or using Unix Cron expression to define more flexible schedule to execute
your job. 

Elasticsearch plugin developers can easily extend JobScheduler plugin to schedule jobs like running
aggregation query against raw data and save the aggregated data into a new index every hour, or keep
monitoring the shard allocation by calling Elasticsearch API and post the output to a Webhook.

## Documentation
TODO

## Build
The JobScheduler plugin uses the [Gradle](https://docs.gradle.org/4.10.2/userguide/userguide.html)
build system.
1. Checkout this package from version control.
1. To build from command line set `JAVA_HOME` to point to a JDK >=11 
1. Run `./gradlew build`

Then you will find the built artifact located at `build/distributions` directory

## Install
Once you have built the plugin from source code, run
```bash
elasticsearch-plugin install file://${PLUGIN_ZIP_FILE_PATH}
```
to install the JobScheduler plugin to your Elasticsearch.

## Develop a plugin that extends JobScheduler
JobScheduelr plugin provides a SPI for other plugins to implement. Essentially, you need to
1. Define your *JobParameter* type by implementing `ScheduledJobParameter` interface
1. Implement your JobParameter parser function that can deserialize your JobParameter from XContent
1. Create your *JobRunner* implementation by implementing `ScheduledJobRunner` interface
1. Create your own plugin which implements `JobSchedulerExtension` interface
   - don't forget to create the service provider configuration file in your resources folder and
   bundle it into your plugin artifact
   
Please refer to the `sample-extension-plugin` subproject in this project, which provides a complete
example of using JobScheduler to run periodical jobs.

The sample extension plugin takes an index name as input and logs the index shards to elasticsearch
logs according to the specified Schedule. And it also exposes a REST endpoint for end users to
create/delete jobs.