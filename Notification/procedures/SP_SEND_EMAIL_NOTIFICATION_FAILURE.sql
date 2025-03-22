!set variable_substitution=true
!variables

CREATE OR REPLACE PROCEDURE CHEM_&{ENV}.BUSINESSANALYTICS.SUPPLY_CHAIN.SP_SEND_EMAIL_NOTIFICATION_FAILURE(
    emailList ARRAY,
    errorCode VARCHAR,
    errorMessage VARCHAR,
    messageId VARCHAR,
    messageType VARCHAR,
    dbName VARCHAR,
    schemaName VARCHAR,
    taskName VARCHAR,
    spName VARCHAR,
    timestamp VARCHAR
)
returns Variant
language python
runtime_version = 3.8
packages = ('snowflake-snowpark-python')
handler = 'send_email_notification_failure'
as
# Author: Hitesh Sahoo
# Email: hitesh.sahoo@shell.com
# Date: 2024-10-30 12:00:00
# Description: Snowpark Python Stored Procedure for sending automated email for failed Tasks of SCBI team

import snowflake.snowpark as snowpark
import pandas as pd
import json

def send_email_notification_failure(session: snowpark.Session, emailList:list, errorCode:str, errorMessage:str, messageId:str, messageType:str, dbName:str, schemaName:str, taskName:str, spName:str, timestamp:str):

    # Your code goes here, inside the "main" handler.
    # The `inputDict` dictionary is created to store the input values for the email notification.
    # It contains key-value pairs where the keys represent the different attributes of the email notification
    # (such as "emails", "errorCode", "errorMessage", etc.) and the values are the corresponding input
    # values passed to the function. This dictionary is later converted into a JSON string using the
    # `json.dumps()` function.

    inputDict = {
        "emails": emailList,
        "errorCode": errorCode,
        "errorMessage": errorMessage,
        "messageId": messageId,
        "messageType": messageType,
        "dbName": dbName,
        "schemaName": schemaName,
        "taskName": taskName,
        "spName": spName,
        "timestamp": timestamp
    }
    print(inputDict)

    jsonString = json.dumps(inputDict, indent=None)
    print("inputDict: " + jsonString)

    # The line `uuid = pd.DataFrame(session.sql("SELECT UUID_STRING();").collect()).iat[0,0]` is
    # executing a SQL query to retrieve a UUID (Universally Unique Identifier) string from the
    # Snowflake database.
    uuid = pd.DataFrame(session.sql("SELECT UUID_STRING();").collect()).iat[0,0]
    print(uuid)
    destination = '@CHEM_&{ENV}.BUSINESSANALYTICS.SUPPLY_CHAIN.STG_NOTIFICATIONS_FAILURE/' + uuid
    destination = str(destination)
    print(destination)

    copyCommand = 'copy into ' + destination + ' from (SELECT COL_VARIANT FROM (SELECT column1 as COL_ID, PARSE_JSON(column2) AS COL_VARIANT FROM VALUES (1, \'''' + jsonString + '''\'))) FILE_FORMAT=(FORMAT_NAME=\'FMT_JSON_FORMAT\' COMPRESSION=NONE);'
    print(str(copyCommand))

    result = session.sql(str(copyCommand)).collect()
    print(result)
    # The return value appears in the Results tab

    return 0

!system echo " Successfully created procedure SP_SEND_EMAIL_NOTIFICATION_FAILURE .... "