# sqlstatscollector - instance database

## Project for collecting statistics on a SQL Server instance
The current full code version works on SQL Server 2012 and forward.
Everything except scheduling and enabling the [connection_properties] collector works in 2008 and 2008 R2.

### collectors

The table [internal].[collectors] contains all collectors available for the collection procedure. Default collection frequency is set in the cron-format.
All collectors except connection_properties are enabled by default.


### collecting data

Collection of data is performed by running the [collect].[run] procedure. 
The setup creates an Agent job in order to schedule the collection.
