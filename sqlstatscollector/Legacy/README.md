# sqlstatscollector - Legacy

## Changes made to be compatible with older SQL Servers
The current full code version works on SQL Server 2012 and forward.

### [connection_properties]  

This change is actually made in the [internal].[collector_status] procedure, due to the fact that SQL 2008 and 2008 R2 are missing parameters for the ring buffer target in Extended Events.

