# sqlstatscollector

## Project for collecting statistics on a SQL Server instance
The current full code version works on SQL Server 2012 and forward.
- SQL Server 2008/2008 R2 see the code in the Legacy folder. Currently there is only the xevent functionality in [status_of_collectors] that doesn't work.
- SQL 2005 and before is likely to never be implemented.

## Project parts

### sqlstatscollector

This is the folder of the instance database. This database should exist on all SQL Server instances that should use data collection.

### dataconsolidation

The code in this directory is installed on the central server, where the sqlstatsmain database is located. 
Currently there is a Powershell script that connects to all 

## Project parts to be created:

- the central database - a database that collects all instances data
- the web UI - lets you add instances for collection to the central database
- the powershell - script for collecting all data
- the reports - a view of the statistics and potential issues in the database

## old project parts (to be removed)

- database
- helperscripts
- powershell
- scheduling
