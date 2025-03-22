!set variable_substitution=true
!variables

-- ======================================
-- Author: Hitesh Sahoo
-- Created date: 2024-10-30
-- Description: It stores json file when email triggers for any failed Tasks for SCBI team
-- ======================================

CREATE OR REPLACE STAGE "CHEM_&{ENV}.BUSINESSANALYTICS"."SUPPLY_CHAIN".STG_NOTIFICATIONS_FAILURE
    URL='azure://<container url goes here>/notifications-failure'
    storage_integration = INT_STORAGE_BUSINESSANALYTICS
    FILE_FORMAT = "CHEM_&{ENV}.BUSINESSANALYTICS"."SUPPLY_CHAIN".FMT_JSON_FORMAT
    COPY_OPTIONS=(ON_ERROR='ABORT_STATEMENT');

!system echo " Successfully created external stage STG_NOTIFICATIONS_FAILURE .... "