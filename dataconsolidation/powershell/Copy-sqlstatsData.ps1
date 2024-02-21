function Copy-AllTableData {

    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true,ValueFromPipelineByPropertyName=$true)]
        [ValidateNotNullOrEmpty()]
        [Alias("SourceServer", "s")]
        [String]$Source,
        [Parameter(Mandatory = $true,ValueFromPipelineByPropertyName=$true)]
        [ValidateNotNullOrEmpty()]
        [String]$key
    )
    

    begin {
        [string]$SourceConStr = $Source
        [string]$TargetConStr = "Server=.;Database=sqlstatscentral;Integrated Security=True"
    }
    

    process {
        
        $collectorcon = New-Object System.Data.SqlClient.SqlConnection
        $collectorcon.ConnectionString = $SourceConStr

        $TableCmd = New-Object System.Data.SqlClient.SqlCommand
        $TableCmd.CommandText = $("[internal].[collectors_for_transfer]")
        $TableCmd.CommandType = [System.Data.CommandType]::StoredProcedure
        $TableCmd.Connection = $collectorcon

        $TableSqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
        $TableSqlAdapter.SelectCommand = $TableCmd

        $TableDataSet = New-Object System.Data.DataSet
        $tablecount = $TableSqlAdapter.Fill($TableDataSet)
        $datacollectors = $TableDataSet.Tables[0]

        $collectorcon.Close()

        $serverid = $datacollectors.Rows[0].serverid
        "Connected to " + $serverid + " - " + $key

        $serveridcon = New-Object System.Data.SqlClient.SqlConnection
        $serveridcon.ConnectionString = $TargetConStr
        $serveridCmd = New-Object System.Data.SqlClient.SqlCommand
        $serveridCmd.CommandText = $("UPDATE [internal].[sqlserverinstances] SET [LastConnection] = SYSUTCDATETIME(), [serverid] = '" + $serverid + "' WHERE [serverkey] = " + $key)
        $serveridCmd.Connection = $serveridcon
        $serveridcon.Open()
        $serveridCmd.ExecuteNonQuery()
        $serveridcon.Close()

        $sourcecon = New-Object -TypeName System.Data.SqlClient.SqlConnection $SourceConStr
        $sourcecon.Open()

        $SqlBulkCopy = New-Object -TypeName System.Data.SqlClient.SqlBulkCopy($TargetConStr, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
        $SqlBulkCopy.EnableStreaming = $true
        $SqlBulkCopy.BatchSize = 1000000
        $SqlBulkCopy.BulkCopyTimeout = 0

        foreach ($row in $datacollectors) { 

            $procedurename = "[transfer]." + $row.collector
            $tablename = "[incoming]." + $row.collector
            "Transferring data to " + $tablename + " using " + $procedurename

            $SqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $SqlCommand.CommandText = $($procedurename)
            $SqlCommand.Connection = $sourcecon
            $SqlCommand.CommandType = [System.Data.CommandType]::StoredProcedure
            [System.Data.SqlClient.SqlDataReader]$SqlReader = $SqlCommand.ExecuteReader()

            $SqlBulkCopy.DestinationTableName = $tablename
            $SqlBulkCopy.WriteToServer($SqlReader)

            $SqlReader.Close()
        }

        $SqlBulkCopy.Close()
        $sourcecon.Close()
    }
    

    end {

        
    }

}




$collectorcon = New-Object System.Data.SqlClient.SqlConnection
$collectorcon.ConnectionString = "Server=.;Database=sqlstatscentral;Integrated Security=True"

$TableCmd = New-Object System.Data.SqlClient.SqlCommand
$TableCmd.CommandText = $("SELECT [ConnectionString], serverkey = CAST([serverkey] AS nvarchar(18)) FROM [internal].[sqlserverinstances] WHERE [IsActive] = 1")
$TableCmd.Connection = $collectorcon

$TableSqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$TableSqlAdapter.SelectCommand = $TableCmd

$TableDataSet = New-Object System.Data.DataSet
$tablecount = $TableSqlAdapter.Fill($TableDataSet)
$datacollectors = $TableDataSet.Tables[0]

$collectorcon.Close()


foreach ($row in $datacollectors) { 

      $constr = $row.ConnectionString
      $key = $row.serverkey
      "Transferring data from " + $constr + " (server = " + $key + ")"
      Copy-AllTableData -Source $constr -key $key

}











