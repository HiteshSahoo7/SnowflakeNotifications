!set variable_substitution=true
!variables

CREATE OR REPLACE PROCEDURE CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_NOTIFICATION_SCBI_TASK_FAILURE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER 
AS 
$$

/**************************************************************************************************************************
* Procedure Name    :  SP_NOTIFICATION_SCBI_TASK_FAILURE                              
* This procedure is used to check if any Task from SCBI project fails, and then triggers mail to relevant team
**************************************************************************************************************************
* Date            Description of Change                                  						Change by     
**************************************************************************************************************************  
* 2024-10-30       Initial Creation        						                                Hitesh Sahoo
**************************************************************************************************************************/



//create Temp table for failed Task-Team mapping
//So that failed task can notify to relevant team like Logistic and SNOP Market Back
//Excel Mapping mantained for all task and converted into csv and stored in container 
var ext_tb_refresh = `ALTER EXTERNAL TABLE CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.EXT_TB_SNOP_LOGISTICS_TASK_MAPPING REFRESH;`

snowflake.execute ({sqlText: ext_tb_refresh});

var tmp_snop_logistics_task_mapping = `CREATE OR REPLACE TEMPORARY TABLE CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TMP_SNOP_LOGISTICS_TASK_MAPPING AS
SELECT 
	UPPER(TRIM(value:c1::varchar)) as TASK_NAME,
	value:c2::varchar as DB_SCHEMA,
	iff(value:c3::varchar = 'Logistics-Packaged','Logistics-Packaged','SNOP-Market Back' ) as TEAM
FROM 
CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.EXT_TB_SNOP_LOGISTICS_TASK_MAPPING;`

snowflake.execute ({sqlText: tmp_snop_logistics_task_mapping});


//create Temp table for look up on previous failed SP
var fail_temp_table = `CREATE OR REPLACE TEMPORARY TABLE FAILURE AS 
                   SELECT ROW_NUMBER() OVER (ORDER BY T.QUERY_ID) AS ID,
                    T.QUERY_ID,T.DATABASE_NAME,T.SCHEMA_NAME,NAME AS TASK_NAME, REGEXP_SUBSTR(T.QUERY_TEXT, '[^.]+$') AS SP_NAME,
                    T.ERROR_CODE,REPLACE(T.ERROR_MESSAGE, '''' ,'') AS ERROR_MESSAGE,T.COMPLETED_TIME,M.TEAM,CURRENT_TIMESTAMP AS LOAD_TIME
                        FROM CHEM_&{ENV}_CURATED.COMMON.TASK_HISTORY T JOIN CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TMP_SNOP_LOGISTICS_TASK_MAPPING M
                        ON T.NAME = M.TASK_NAME 
                            WHERE T.DATABASE_NAME IN ('CHEM_PRD_BUSINESSANALYTICS','CHEM_PRD_ANALYTICS')
                            AND T.SCHEMA_NAME IN ('SUPPLY_CHAIN','OTD')
                            AND T.STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED','CANCELLED','SKIPPED')
                            AND DATE(T.COMPLETED_TIME) = CURRENT_DATE
                            AND T.QUERY_ID NOT IN (SELECT QUERY_ID FROM CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_NOTIFICATION_FAILURE)
                            ORDER BY T.COMPLETED_TIME ;`

                    snowflake.execute ({sqlText: fail_temp_table});

//Count of rows 
var fail_query = `SELECT * FROM FAILURE;`
var fail_count = snowflake.execute ({sqlText: fail_query});
var count = fail_count.getRowCount();  

//if no new task failed in scheduled time then count is 0 and loop fails
for (i = 1; i <= fail_count.getRowCount(); i++) 
    {                                    
        var ec = `SELECT ERROR_CODE FROM FAILURE WHERE ID = '${i}';`
        store_ec = snowflake.execute ({sqlText: ec});   
        store_ec.next();    
        error_code = store_ec.getColumnValue(1);
        
        var msg = `SELECT regexp_replace(ERROR_MESSAGE, '[^a-zA-Z, ._]+', '') AS ERROR_MESSAGE FROM FAILURE WHERE ID = ${i};`
        store_msg = snowflake.execute ({sqlText: msg});   
        store_msg.next();    
        error_message = store_msg.getColumnValue(1);

        var db = `SELECT DATABASE_NAME FROM FAILURE WHERE ID = ${i};`
        store_db = snowflake.execute ({sqlText: db});   
        store_db.next();    
        db_name = store_db.getColumnValue(1);

        var sc = `SELECT SCHEMA_NAME FROM FAILURE WHERE ID = ${i};`
        store_sc = snowflake.execute ({sqlText: sc});   
        store_sc.next();    
        schema_name = store_sc.getColumnValue(1);
        
        var tsk = `SELECT TASK_NAME NAME FROM FAILURE WHERE ID = ${i};`
        store_tsk = snowflake.execute ({sqlText: tsk});   
        store_tsk.next();    
        task_name = store_tsk.getColumnValue(1);
        
        var sp = `SELECT SP_NAME FROM FAILURE WHERE ID = ${i};`
        store_sp = snowflake.execute ({sqlText: sp});   
        store_sp.next();    
        sp_name = store_sp.getColumnValue(1);

        var team = `SELECT TEAM FROM FAILURE WHERE ID = ${i};`
        store_team = snowflake.execute ({sqlText: team});   
        store_team.next();    
        team_name = store_team.getColumnValue(1);

       if (team_name == 'Logistics-Packaged') {
        var call_notification = `CALL CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_SEND_EMAIL_NOTIFICATION_FAILURE(
						  ARRAY_CONSTRUCT('T&&E-EMIT-SCCOT-SCDA-ANALYTICS.UG@shell.com','SCBI-ANALYTICS-LOGISTICS-PACKAGED.UG@shell.com','hitesh.sahoo@shell.com'),
                          '${error_code}',
                          '${error_message}',
                          (select uuid_string()),
                          'TASK FAILED',
                          '${db_name}',
                          '${schema_name}',
                          '${task_name}',
                          '${sp_name}',
                          concat((select current_timestamp), ' Los Angeles, USA')
						  );`;
        snowflake.execute ({sqlText:  call_notification});
        }
       
       if (team_name == 'SNOP-Market Back' ) {
        var call_notification=`CALL CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.SP_SEND_EMAIL_NOTIFICATION_FAILURE(
                    ARRAY_CONSTRUCT('T&&E-EMIT-SCCOT-SCDA-ANALYTICS.UG@shell.com','SCBI-ANALYTICS-S&&OP-MARKET-BACK.UG@shell.com','hitesh.sahoo@shell.com'),
                          '${error_code}',
                          '${error_message}',
                          (select uuid_string()),
                          'TASK FAILED',
                          '${db_name}',
                          '${schema_name}',
                          '${task_name}',
                          '${sp_name}',
                          concat((select current_timestamp), ' Los Angeles, USA'  ));`
        snowflake.execute({sqlText:  call_notification});
        
       
       }   
}


var fail_insert = `INSERT INTO CHEM_&{ENV}_BUSINESSANALYTICS.SUPPLY_CHAIN.TB_NOTIFICATION_FAILURE 
                        SELECT * FROM FAILURE;`

                    snowflake.execute ({sqlText: fail_insert});

return "Total Task Failed : "+count;

$$;

!system echo "  Successfully created procedure SP_NOTIFICATION_SCBI_TASK_FAILURE  .... "