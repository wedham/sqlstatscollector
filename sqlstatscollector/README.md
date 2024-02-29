# sqlstatscollector - instance database

## Project for collecting statistics on a SQL Server instance

The current full code version works on SQL Server 2012 and forward.
Everything except scheduling and enabling the [connection_properties] collector works in 2008 and 2008 R2.

### collectors

The table [internal].[collectors] contains all collectors available for the collection procedure. Default collection frequency is set in the cron-format.
All collectors except connection_properties are enabled by default.

#### server_properties

Collects details about the current SQL Server instance.

#### server_stats

Lists current server statistics over time

#### memory_stats

Basic memory usage over time

#### cpu_stats

CPU information. Both instance CPU use and machine idle CPU over time.

#### wait_stats

Waiting tasks statistics over time

#### database_properties

Information about all databases, including backup info and DBCC details.

#### databasefile_properties

Size, growth and other properties of database files.

#### databasefile_stats

IO statistics per collection interval on all current database files.

#### database_memory_usage

Memory usage of each database over time.

#### database_cpu_usage

CPU usage of each database over time.

#### connection_properties

Collects all database locks taken by connections. *disabled by default* (Uses Extended events)

#### job_properties

Gets job information and run-times for scheduled jobs

#### availability_group_properties

Collects details about Always On Availability Groups

### collecting data

Collection of data is performed by running the [collect].[run] procedure.
The setup creates an Agent job in order to schedule the collection.
