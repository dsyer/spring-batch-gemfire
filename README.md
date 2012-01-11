# Spring Batch Gemfire Integration #

This project provides some implementations of Spring Batch idioms and SPIs using [SpringSource Gemfire](http://www.springsource.com/products/data-management).

## Getting Started ##

To build it clone and then use Maven:

    $ git clone ...
    $ cd spring-batch-gemfire
    $ mvn install

To use it you would create a project depend on both Spring Batch and this project.  If you have the SpringSource Tool Suite (STS) you can use  a template project (File->New->Spring Template Project) to get a starting point.

Using Maven: in your `pom.xml`

		<dependency>
			<groupId>org.springframework.batch</groupId>
			<artifactId>spring-batch-gemfire</artifactId>
			<version>1.0.0.BUILD-SNAPSHOT</version>
		</dependency>

and then you will be able to use all the features provided by this project.

## Features ##

`GemfirePartitionHandler` can be used to send Step execution work out into a Gemfire cluster.  There is an example in the spring-batch-gemfire unit tests:

    <job id="job" xmlns="http://www.springframework.org/schema/batch">
        <step id="step-master">
            <partition handler="partitionHandler" step="step"
                partitioner="partitioner" />
        </step>
    </job>
    <bean id="partitionHandler" class="org.springframework.batch.core.partition.gemfire.GemfirePartitionHandler">
        <property name="region" ref="region" />
        <property name="gridSize" value="20" />
        <property name="step" ref="step" />
    </bean>

See the unit test and the source code of the handler for more detail.  The test is very minimal (it uses hard-coded in memory data, not distributed cache data), but it should scale well and Gemfire should take care of data locality if an item reader that reads from the cache is used.  The test is set up to run in-memory (not distributed) by default, so you can check that everything is working by simply running it out of the box.  To run in a small cluster you can use the `RegionLauncher` provided in the same package to launch another gem node:

1. Start an HSQL database server with connection url `jdbc:hsqldb:hsql://localhost:9005/samples`.  There is a Maven profile for this,
so you can run `$ mvn -P rundb exec:java` from the command line.  From Eclipse if you imported
this project as a Maven project, you should be able to use the launcher in the top level directory - right click and run as...
2. Run the `RegionLauncher` (it will fail if the database is not running)
3. Change the test case so the context location is `launch-context.xml`
4. Run the test

You should see some steps being processed in the console output from the `RegionLauncher`, and you should see the test succeed.  The `RegionLauncher` log output will also show the partition executions being loaded and unloaded from the cluster through its cache listener.
