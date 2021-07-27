SIMPLE_SELECT_EXPLAINED_QUERY = {
    "ResponseMetadata": {
        "HTTPHeaders": {
            "connection": "keep-alive",
            "content-length": "33",
            "content-type": "application/x-amz-json-1.1",
            "date": "Fri, 09 Jul 2021 05:34:59 " "GMT",
            "x-amzn-requestid": "xxx",
        },
        "HTTPStatusCode": 200,
        "RequestId": "xxxx",
        "RetryAttempts": 0,
    },
    "ResultSet": {
        "ResultSetMetadata": {
            "ColumnInfo": [
                {
                    "CaseSensitive": True,
                    "CatalogName": "hive",
                    "Label": "Query Plan",
                    "Name": "Query Plan",
                    "Nullable": "UNKNOWN",
                    "Precision": 781,
                    "Scale": 0,
                    "SchemaName": "",
                    "TableName": "",
                    "Type": "varchar",
                }
            ]
        },
        "Rows": [
            {"Data": [{"VarCharValue": "Query Plan"}]},
            {"Data": [{"VarCharValue": "- Output[col1, col2] => " "[[col1, col2]]"}]},
            {"Data": [{"VarCharValue": "    - Limit[10] => [[col1, " "col2]]"}]},
            {
                "Data": [
                    {
                        "VarCharValue": "        - "
                        "LocalExchange[SINGLE] "
                        "() "
                        "=> [[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "            - "
                        "RemoteExchange["
                        "GATHER] => "
                        "[[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                - "
                        "LimitPartial[10] => "
                        "[[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                    - "
                        "TableScan["
                        "awsdatacatalog:HiveTableHandle{schemaName=foo, "
                        "tableName=bar, "
                        "analyzePartitionValues=Optional.empty}] "
                        "=> [[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                    - "
                        "TableScan["
                        "awsdatacatalog:HiveTableHandle{schemaName=baz, "
                        "tableName=qux, "
                        "analyzePartitionValues=Optional.empty}] "
                        "=> [[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {"VarCharValue": "                            " "LAYOUT: foo.bar"}
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                            "
                        "col1 := "
                        "col1:bigint:1:REGULAR"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                            "
                        "col2 := "
                        "col2:bigint:0:REGULAR"
                    }
                ]
            },
            {"Data": [{"VarCharValue": ""}]},
            {"Data": [{"VarCharValue": ""}]},
        ],
    },
}

SIMPLE_SELECT_EXPLAINED_SELF_JOIN_QUERY = {
    "ResponseMetadata": {
        "HTTPHeaders": {
            "connection": "keep-alive",
            "content-length": "33",
            "content-type": "application/x-amz-json-1.1",
            "date": "Fri, 09 Jul 2021 05:34:59 " "GMT",
            "x-amzn-requestid": "xxx",
        },
        "HTTPStatusCode": 200,
        "RequestId": "xxxx",
        "RetryAttempts": 0,
    },
    "ResultSet": {
        "ResultSetMetadata": {
            "ColumnInfo": [
                {
                    "CaseSensitive": True,
                    "CatalogName": "hive",
                    "Label": "Query Plan",
                    "Name": "Query Plan",
                    "Nullable": "UNKNOWN",
                    "Precision": 781,
                    "Scale": 0,
                    "SchemaName": "",
                    "TableName": "",
                    "Type": "varchar",
                }
            ]
        },
        "Rows": [
            {"Data": [{"VarCharValue": "Query Plan"}]},
            {"Data": [{"VarCharValue": "- Output[col1, col2] => " "[[col1, col2]]"}]},
            {"Data": [{"VarCharValue": "    - Limit[10] => [[col1, " "col2]]"}]},
            {
                "Data": [
                    {
                        "VarCharValue": "        - "
                        "LocalExchange[SINGLE] "
                        "() "
                        "=> [[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "            - "
                        "RemoteExchange["
                        "GATHER] => "
                        "[[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                - "
                        "LimitPartial[10] => "
                        "[[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                    - "
                        "TableScan["
                        "awsdatacatalog:HiveTableHandle{schemaName=foo, "
                        "tableName=bar, "
                        "analyzePartitionValues=Optional.empty}] "
                        "=> [[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                    - "
                        "TableScan["
                        "awsdatacatalog:HiveTableHandle{schemaName=foo, "
                        "tableName=bar, "
                        "analyzePartitionValues=Optional.empty}] "
                        "=> [[col1, col2]]"
                    }
                ]
            },
            {
                "Data": [
                    {"VarCharValue": "                            " "LAYOUT: foo.bar"}
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                            "
                        "col1 := "
                        "col1:bigint:1:REGULAR"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "                            "
                        "col2 := "
                        "col2:bigint:0:REGULAR"
                    }
                ]
            },
            {"Data": [{"VarCharValue": ""}]},
            {"Data": [{"VarCharValue": ""}]},
        ],
    },
}
