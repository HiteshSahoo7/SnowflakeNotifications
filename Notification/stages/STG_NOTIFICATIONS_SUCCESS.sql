!set variable_substitution=true
!variables

-- ======================================
-- Author: Hitesh Sahoo
-- Created date: 2024-10-30
-- Description: It stores json file when email triggers after all Demand Planning task completion
-- ======================================

CREATE OR REPLACE STAGE "CHEM_&{ENV}.BUSINESSANALYTICS"."SUPPLY_CHAIN".​STG_NOTIFICATIONS_SUCCESS
    URL='azure://<container url goes here>/notifications-success'
    storage_integration = INT_STORAGE_BUSINESSANALYTICS
    FILE_FORMAT = "CHEM_&{ENV}.BUSINESSANALYTICS"."SUPPLY_CHAIN".FMT_JSON_FORMAT
    COPY_OPTIONS=(ON_ERROR='ABORT_STATEMENT');

!system echo " Successfully created external stage STG_NOTIFICATIONS_SUCCESS .... "