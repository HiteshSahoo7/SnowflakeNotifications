!set variable_substitution=true
!variables

CREATE OR REPLACE PROCEDURE CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_SEND_EMAIL_NOTIFICATION_SUCCESS(
    emailList ARRAY,
    successMessage VARCHAR,
    messageType VARCHAR,
    taskName VARCHAR,
    timestamp VARCHAR
)
returns Variant
language python
runtime_version = 3.8
packages = ('snowflake-snowpark-python')
handler = 'send_email_notification_success'
AS '
# Author: Hitesh Sahoo
# Email: hitesh.sahoo@shell.com
# Date: 2024-10-30 12:00:00
# Description: Snowpark Python Stored Procedure for sending automated email for all Demand Planning success Tasks

import snowflake.snowpark as snowpark
import pandas as pd
import json

def send_email_notification_success(session: snowpark.Session, emailList:list, successMessage:str, messageType:str, taskName:str, timestamp:str):

    # Your code goes here, inside the "main" handler.
    # The `inputDict` dictionary is created to store the input values for the email notification. 
    # It contains key-value pairs where the keys represent the different attributes of the email notification 
    # (such as "emails", "errorCode", "successMessage", etc.), and the values are the corresponding input values passed to the function.
    # This dictionary is later converted into a JSON string using the `json.dumps()` function.

    inputDict = {
        "emails": emailList,
        "successMessage": successMessage,
        "messageType": messageType,
        "taskName": taskName,
        "timestamp": timestamp
    }

    print(inputDict)

    jsonString = json.dumps(inputDict, indent=None)
    print("inputDict: " + jsonString)

    # The line `uuid = pd.DataFrame(session.sql("SELECT UUID_STRING();").collect())[0,0]` is 
    # executing a SQL query to retrieve a UUID (Universally Unique Identifier) string from the Snowflake database.
    uuid = pd.DataFrame(session.sql("SELECT UUID_STRING();").collect()).iat[0,0]

    print(uuid)

    destination = "CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.STG_NOTIFICATION_SUCCESS/" + uuid
    destination = str(destination)

    print(destination)

    copyCommand = "copy into " + destination + " from (SELECT column1 as COL_ID, PARSE_JSON(column2) AS COL_VARIANT FROM VALUES (1, \\'''" + jsonString + "\\''')) FILE_FORMAT=(FORMAT_NAME='MY_JSON_FORMAT') COMPRESSION='GZIP'"

    print(str(copyCommand))

    result = session.sql(str(copyCommand)).collect()
    print(result)

    # The return value appears in the Results tab
    return 0
';

!system echo " Successfully created procedure SP_SEND_EMAIL_NOTIFICATION_SUCCESS .... "