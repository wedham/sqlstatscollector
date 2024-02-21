# sqlstatscollector

## dataconsolidation

This folder contains the code that can be used to consolidate the information from unique sqlstatscollector databases into the central sqlstatsmain database.

### powershell

Copy-sqlstatsData.ps1

Loops all instances registered in [internal].[sqlserverinstances] and collect the data from all collectors into the [incoming] schema of the sqlstatsmain database.
