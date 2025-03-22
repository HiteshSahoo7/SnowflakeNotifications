!set variable_substitution=true
!variables

CREATE OR REPLACE PROCEDURE CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_NOTIFICATION_DP_TASK_SUCCESS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER 
AS 
$$

/**************************************************************************************************************************
* Procedure Name    :  SP_NOTIFICATION_DP_TASK_SUCCESS                              
* This procedure is used to check if all 8 Tasks execute successfully for Data Processing project
* on WD1 and WD2 and then send mail to WD1/WD2 audience automatic rather sending manually 
**************************************************************************************************************************
* Date            Description of Change                                  						Change by     
**************************************************************************************************************************  
* 2024-10-30       Initial Creation        						                                Hitesh Sahoo
**************************************************************************************************************************/


// Function to get current date in YYYY-MM-DD format for a given time zone
function getCurrentDateInTimeZone(timeZone) {
    return new Date().toLocaleDateString('en-CA', { timeZone });
}

// Fetching current date in Los Angeles (Mountain Standard Time)
const LAdate = getCurrentDateInTimeZone('America/Los_Angeles');

// Fetching Current Month and Year
var [Year, Month] = LAdate.split('-');
currentMonth=parseInt(Month);
currentYear=parseInt(Year);
 
wd1 = snowflake.createStatement( {sqlText: 
"SELECT CALENDAR_DATE, 'WD1' AS DAY FROM CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_SC_WORKDAY_CALENDAR 
WHERE WORKDAY = '1' AND DATE_PART(Month,CALENDAR_DATE::date)='" + currentMonth + "' AND DATE_PART(Year,CALENDAR_DATE::date)='" + currentYear + "' ;"}). execute();

wd2 = snowflake.createStatement( {sqlText: 
"SELECT CALENDAR_DATE,'WD2' AS DAY FROM CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_SC_WORKDAY_CALENDAR 
WHERE WORKDAY = '2' AND DATE_PART(Month,CALENDAR_DATE::date)='" + currentMonth + "' AND DATE_PART(Year,CALENDAR_DATE::date)='" + currentYear + "' ;"}). execute();

wd1.next();
var dbdate1 = wd1.getColumnValue(1); // returns WD1 Date
var day1 = wd1.getColumnValue(2); // retruns WD1 as DAY from above query

wd2.next();
var dbdate2 = wd2.getColumnValue(1); // returns WD2 Date
var day2 = wd2.getColumnValue(2); //retruns WD2 as DAY from above query

//Success SQL query to create Temp table with all task completion of Data Processing i.e. 8 tasks
var success_temp_table  = `CREATE OR REPLACE TEMPORARY TABLE SUCCESS AS 
                    SELECT QUERY_ID,NAME, DATABASE_NAME, SCHEMA_NAME, STATE, COMPLETED_TIME, SCHEDULED_TIME
                    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(ROOT_TASK_ID=>'01afdc79-36a3-d01e-0000-000000001f74'))
                        WHERE 1=1
                        AND DATABASE_NAME = 'CHEM_PRD_BUSINESSANALYTICS'
                        AND SCHEMA_NAME = 'SUPPLY_CHAIN'
                        AND STATE = 'SUCCEEDED'
                        AND DATE(COMPLETED_TIME) = DATE(CONVERT_TIMEZONE('America/Los_Angeles',CURRENT_TIMESTAMP())) 
                        -- Los Angeles Timezone might not needed if date is as per Los Angeles in js i.e. LAdate
                        AND DATE(COMPLETED_TIME) NOT IN (SELECT DATE(COMPLETED_TIME) FROM CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_NOTIFICATION_SUCCESS)
                        --Above filter is to restrict duplicates flowing into TB_NOTIFICATION_SUCCESS and retriggering email again and again 
                        ORDER BY COMPLETED_TIME DESC;`

            snowflake.execute ({sqlText: success_temp_table});

//Count of rows 
var success_query = `SELECT * FROM SUCCESS;`
var success_query_exe = snowflake.execute ({sqlText: success_query});
var success_count = success_query_exe.getRowCount();  

//Check for WD1 for Los Angeles else email might trigger due to timezone mismatch 
if (dbdate1 == LAdate )
{
    if (success_count == 8) {
        var call_notification=`CALL CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_SEND_EMAIL_NOTIFICATION_SUCCESS(
                                              ARRAY_CONSTRUCT('T&&E-EMIT-SCCOT-SC-S&&OP-EAST-WD1&&WD2.UG@shell.com','hitesh.sahoo@shell.com'),
                                              'SuccessMessage',
                                              concat('${day1}',' Updated4'),
                                              'Dashboard Data Ingestion Snowflake completed',
                                              (select current_timestamp));`
                            snowflake.execute({sqlText:  call_notification});


//insert first triggered row of WD1 into below table, so that it can look up on COMPLETED_TIME date and will not send repeated mail for WD1
var success_insert_wd1 = `INSERT INTO CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_NOTIFICATION_SUCCESS(DAY, COMPLETED_TIME, COUNT_TASK, LOAD_TIME) 
                            SELECT '${day1}' AS DAY, 
                            MAX(COMPLETED_TIME) AS COMPLETED_TIME,
                            COUNT(*) AS COUNT_TASK, 
                            CURRENT_TIMESTAMP AS LOAD_TIME
                        FROM SUCCESS
                            GROUP BY ALL
                            ORDER BY COMPLETED_TIME DESC;`

                    snowflake.execute ({sqlText: success_insert_wd1});
       
  }
  else {
    return "Already Mail sent for WD1" + LAdate + " | Or Task in progress with Count: " + success_count + "/8"
}}

//Check for WD2 and Send Email
if (dbdate2 == LAdate)
{
    if (success_count == 8) {
        var call_notification=`CALL CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_SEND_EMAIL_NOTIFICATION_SUCCESS(
                                              ARRAY_CONSTRUCT('T&&E-EMIT-SCCOT-SC-S&&OP-EAST-WD1&&WD2.UG@shell.com','hitesh.sahoo@shell.com'),
                                              'SuccessMessage',
                                              concat('${day2}',' Updated4'),
                                              'Dashboard Data Ingestion Snowflake completed',
                                              (select current_timestamp));`
                            snowflake.execute({sqlText:  call_notification});

//insert first triggered row of WD2 into below table, so that it can look up on COMPLETED_TIME date and will not send repeated mail for WD2
var success_insert_wd2 = `INSERT INTO CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_NOTIFICATION_SUCCESS(DAY, COMPLETED_TIME, COUNT_TASK, LOAD_TIME) 
                            SELECT '${day2}' AS DAY, 
                            MAX(COMPLETED_TIME) AS COMPLETED_TIME,
                            COUNT(*) AS COUNT_TASK, 
                            CURRENT_TIMESTAMP AS LOAD_TIME
                        FROM SUCCESS
                            GROUP BY ALL
                            ORDER BY COMPLETED_TIME DESC;`

                    snowflake.execute ({sqlText: success_insert_wd2});
        
  }
  else {
    return "Already Mail sent for WD2" + LAdate + " | Or Task in progress with Count: " + success_count + "/8"
}}
$$;

!system echo "  Successfully created procedure SP_NOTIFICATION_DP_TASK_SUCCESS  .... "