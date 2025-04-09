import datetime
from typing import Dict, List, Optional, Set

import pytest

import datahub.metadata.schema_classes as models
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigQueryTableRef,
    QueryEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryV2Config,
    GcsLineageProviderConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.ingestion.source.bigquery_v2.lineage import (
    BigqueryLineageExtractor,
    LineageEdge,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.sql_parsing.schema_resolver import SchemaResolver


@pytest.fixture
def lineage_entries() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="""
                INSERT INTO `my_project.my_dataset.my_table`
                SELECT first.a, second.b FROM `my_project.my_dataset.my_source_table1` first
                LEFT JOIN `my_project.my_dataset.my_source_table2` second ON first.id = second.id
            """,
            statementType="INSERT",
            project_id="proj_12344",
            end_time=None,
            referencedTables=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table1"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table2"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
            project_id="proj_12344",
            end_time=datetime.datetime.fromtimestamp(
                1617295943.17321, tz=datetime.timezone.utc
            ),
            referencedTables=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table3"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_view1"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
    ]


FAILED_ATLAN_QUERY = """
create or replace table `itp-aia-datalake.asr_selfserve.tb_asr_key_account_stakeholders` as (
    WITH key_account_stakeholders as (SELECT
        recordtype.name as name,
        question.Question_Text__c as Question_Text,
        question.type__c as type,
        Account__c as account_code,
        account.name as account_name,
        IFNULL(answer.Title__c, '') as title,
        IFNULL(Stakeholder_Name__c, '') as Stakeholder_id,
        IFNULL(contact.name, '') as Stakeholder_Name,
        IFNULL(Buying_Role__c, '') as Buying_Role,
        IFNULL(Buyer_Focus__c, '') as Buyer_Focus,
        IFNULL(Speedboat__c, '') as Speedboat,
        IFNULL(CISO_Type__c, '') as CISO_Type,
        IFNULL(PANW_Contact__c, '') as PANW_Contact_id,
        IFNULL(user.name, '') as PANW_Contact_Name,
        IFNULL(Strength_of_Relationship__c, '') as Strength_of_Relationship,
        IFNULL(Answer_Text__c, '') as Answer_Text,
        account_stard.acct_territory_theatre,
        account_stard.acct_territory_area,
        account_stard.acct_territory_region,
        account_stard.acct_territory_district,
        account_stard.account_territory_owner,
        account_stard.acct_territory_name,
        security2000_ranking__c as strategy_account_ranking
        FROM
        `itp-aia-datalake.sfdc.account_strategy_answer__c` answer
        LEFT JOIN (SELECT  * FROM `itp-aia-datalake.sfdc.account_strategy_question__c` ) question
            ON answer.account_strategy_question__c = question.id
        LEFT JOIN (SELECT * FROM `itp-aia-datalake.sfdc.recordtype`) recordtype
            ON recordtype.id  = question.recordtypeid
        LEFT JOIN (select id, name from `itp-aia-datalake.sfdc.user`) user
            ON PANW_Contact__c = user.id
        LEFT JOIN (select id, name from `itp-aia-datalake.sfdc.contact`) contact
            ON Stakeholder_Name__c = contact.id
        LEFT JOIN (select id, name, security2000_ranking__c from `itp-aia-datalake.sfdc.account`) account
            ON answer.Account__c = account.id
        LEFT JOIN (select * from `itp-aia-datalake.sales_dm.vw_account_stard_ssot`) account_stard
            ON answer.Account__c = account_stard.account_id
        WHERE recordtype.name = "Key Account Stakeholders" 
        )
        SELECT
            key_account_stakeholders.* Except(Speedboat),
            individual_speedboat AS Speedboat,
        FROM
            key_account_stakeholders,
            UNNEST(
                CASE 
                    WHEN Speedboat IS NULL THEN [''] 
                    ELSE SPLIT(Speedboat, ',') 
                END
            ) AS individual_speedboat
)
"""


FAILED_ATLAN_QUERY_2 = """
CREATE TEMP FUNCTION TopN(arr ANY TYPE,
    n INT64) AS ( ARRAY(
    SELECT
      x
    FROM
      UNNEST(ARRAY(
        SELECT
          x
        FROM
          UNNEST(arr) AS x
        ORDER BY
          x DESC)) AS x
    WITH
    OFFSET
      off
    WHERE
      off < n
    ORDER BY
      off) );

CREATE OR REPLACE TABLE
  itp-aia-datalake.cortex.tb_tsm_xdr_usage AS
WITH
  base AS (
  SELECT
    *,
    AVG(XDR_Total_EP_License_Count) OVER (PARTITION BY xdr_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 6 PRECEDING
      AND CURRENT ROW ) XDR_Total_EP_License_Count_7_Day_avg,
    AVG(XDR_Total_EP_Agent_Count) OVER (PARTITION BY xdr_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 6 PRECEDING
      AND CURRENT ROW ) XDR_Total_EP_Agent_Count_7D_avg,
    ARRAY_AGG(daily_ingestion_size_gb) OVER (PARTITION BY xdr_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 6 PRECEDING
      AND CURRENT ROW ) daily_ingestion_size_gb_list,
    ARRAY_AGG(xdrgblicensecount) OVER (PARTITION BY xdr_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 6 PRECEDING
      AND CURRENT ROW ) xdrgblicensecount_list
  FROM
    `itp-aia-datalake.cortex.tb_telemetry_XDR_ssot_tmp2_new`)

SELECT
  date,
  xdr_id,
  MAX(XDR_Total_EP_Agent_Count_7D_avg) XDR_Total_EP_Agent_Count_7D_avg,
  ROUND(AVG(list_1),2) GB_Usage_7D_avg,
FROM (
  SELECT
    *,
    TopN(daily_ingestion_size_gb_list,
      4) AS daily_ingestion_size_gb_list_result,
    TopN(xdrgblicensecount_list,
      4) AS xdrgblicensecount_list_result
  FROM
    base),
  UNNEST(daily_ingestion_size_gb_list_result) list_1,
  UNNEST(xdrgblicensecount_list_result) list_2
GROUP BY 1, 2
"""


FAILED_ATLAN_QUERY_3 = """
CREATE OR REPLACE TABLE `itp-aia-datalake.target_dataset.target_table` AS
SELECT
  user_data.id AS user_id,
  user_data.info.email AS user_email,
  user_data.metrics.score AS user_score
FROM
  `itp-aia-datalake.source_dataset.source_table` AS user_data;

"""

FAILED_ATLAN_QUERY_4 = """
create or replace table apex_agg as (
        select * except(apex_is_Partner),
            case 
                  when (apex_is_Partner like '%Mixed%' or apex_is_Partner like '%Partner%PANW%' or apex_is_Partner like '%PANW%Partner%') then 'Mixed' 
                  when apex_is_Partner = 'Partner' then 'Partner'
                  else 'PANW'
                end as apex_is_Partner
            from (
                  select 
                  apexid,
                  string_agg(distinct csp_visitor,';') apex_visitor,
                  string_agg(distinct csp_all_users,';') apex_all_users,
                  string_agg(distinct csp_is_Partner,';') apex_is_Partner, 
                  from csp_data 
                  group by 1
            ) 
  )
"""




@pytest.fixture
def atlan_failed_query_entries() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query=FAILED_ATLAN_QUERY_2,
            statementType="CREATE_TABLE_AS_SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/account_strategy_answer__c"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/account_strategy_question__c"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/recordtype"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/user"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/contact"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/account"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sales_dm/tables/vw_account_stard_ssot"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/asr_selfserve/tables/tb_asr_key_account_stakeholders"
            ),
        ),
    ]


@pytest.fixture
def atlan_failed_query_entries_2() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query=FAILED_ATLAN_QUERY_2,
            statementType="CREATE_TABLE_AS_SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/cortex/tables/tb_telemetry_XDR_ssot_tmp2_new"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/cortex/tables/tb_tsm_xdr_usage"
            ),
        )
    ]

@pytest.fixture
def atlan_failed_query_entries_3() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query=FAILED_ATLAN_QUERY_3,
            statementType="CREATE_TABLE_AS_SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/source_dataset/tables/source_table"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/target_dataset/tables/target_table"
            ),
        )
    ]

@pytest.fixture
def atlan_failed_query_entries_4() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query=FAILED_ATLAN_QUERY_4,
            statementType="CREATE_TABLE_AS_SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/source_dataset/tables/csp_data"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/target_dataset/tables/apex_agg"
            ),
        )
    ]

FAILED_ATLAN_RAHUL_QUERY_1 = """
create or replace table `itp-aia-datalake.asr_selfserve.tb_asr_key_account_stakeholders` as (
    WITH key_account_stakeholders as (SELECT
        recordtype.name as name,
        question.Question_Text__c as Question_Text,
        question.type__c as type,
        Account__c as account_code,
        account.name as account_name,
        IFNULL(answer.Title__c, '') as title,
        IFNULL(Stakeholder_Name__c, '') as Stakeholder_id,
        IFNULL(contact.name, '') as Stakeholder_Name,
        IFNULL(Buying_Role__c, '') as Buying_Role,
        IFNULL(Buyer_Focus__c, '') as Buyer_Focus,
        IFNULL(Speedboat__c, '') as Speedboat,
        IFNULL(CISO_Type__c, '') as CISO_Type,
        IFNULL(PANW_Contact__c, '') as PANW_Contact_id,
        IFNULL(user.name, '') as PANW_Contact_Name,
        IFNULL(Strength_of_Relationship__c, '') as Strength_of_Relationship,
        IFNULL(Answer_Text__c, '') as Answer_Text,
        account_stard.acct_territory_theatre,
        account_stard.acct_territory_area,
        account_stard.acct_territory_region,
        account_stard.acct_territory_district,
        account_stard.account_territory_owner,
        account_stard.acct_territory_name,
        security2000_ranking__c as strategy_account_ranking
        FROM
        `itp-aia-datalake.sfdc.account_strategy_answer__c` answer
        LEFT JOIN (SELECT  * FROM `itp-aia-datalake.sfdc.account_strategy_question__c` ) question
            ON answer.account_strategy_question__c = question.id
        LEFT JOIN (SELECT * FROM `itp-aia-datalake.sfdc.recordtype`) recordtype
            ON recordtype.id  = question.recordtypeid
        LEFT JOIN (select id, name from `itp-aia-datalake.sfdc.user`) user
            ON PANW_Contact__c = user.id
        LEFT JOIN (select id, name from `itp-aia-datalake.sfdc.contact`) contact
            ON Stakeholder_Name__c = contact.id
        LEFT JOIN (select id, name, security2000_ranking__c from `itp-aia-datalake.sfdc.account`) account
            ON answer.Account__c = account.id
        LEFT JOIN (select * from `itp-aia-datalake.sales_dm.vw_account_stard_ssot`) account_stard
            ON answer.Account__c = account_stard.account_id
        WHERE recordtype.name = "Key Account Stakeholders" 
        )
        SELECT
            key_account_stakeholders.* Except(Speedboat),
            individual_speedboat AS Speedboat,
        FROM
            key_account_stakeholders,
            UNNEST(
                CASE 
                    WHEN Speedboat IS NULL THEN [''] 
                    ELSE SPLIT(Speedboat, ',') 
                END
            ) AS individual_speedboat
)
"""

@pytest.fixture
def atlan_failed_query_entries_5() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query=FAILED_ATLAN_RAHUL_QUERY_1,
            statementType="CREATE_TABLE_AS_SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/account_strategy_answer__c"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/account_strategy_question__c"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/recordtype"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/user"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/contact"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sfdc/tables/account"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/sales_dm/tables/vw_account_stard_ssot"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/asr_selfserve/tables/tb_asr_key_account_stakeholders"
            ),
        )
    ]

FAILED_ATLAN_RAHUL_QUERY_2 = """
INSERT INTO `itp-aia-datalake.gcsd_dm.tb_gcs_qualtrics_support_survey`
SELECT *,
       'qualtrics' AS source_type
FROM
  (SELECT *,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_product_osat__c"), r'[^a-za-z0-9.]', '') AS response_product_osat__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_support_osat__c"), r'[^a-za-z0-9.]', '') AS response_support_osat__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_overall_satisfaction__c"), r'[^a-za-z0-9.]', '') AS response_overall_satisfaction__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_team_resolution__c"), r'[^a-za-z0-9.-]', '') AS response_team_resolution__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_product_knowledge__c"), r'[^a-za-z0-9.-]', '') AS response_product_knowledge__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_team_service__c"), r'[^a-za-z0-9.-]', '') AS response_team_service__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_team_understanding__c"), r'[^a-za-z0-9.-]', '') AS response_team_understanding__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_team_improvement__c"), r'["]', '') AS response_team_improvement__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_team_exceptional__c"), r'[^a-za-z0-9.-]', '') AS response_team_exceptional__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_company_recommend__c"), r'[^a-za-z0-9.]', '') AS response_company_recommend__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_recommend_score__c"), r'[^a-za-z0-9.]', '') AS response_recommend_score__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_level_of_effort__c"), r'[^a-za-z0-9 .]', '') AS response_level_of_effort__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_solution_online__c"), r'[^a-za-z0-9.]', '') AS response_solution_online__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_self_service_experience__c"), r'[^a-za-z0-9.]', '') AS response_self_service_experience__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_case_resolved__c"), r'[^a-za-z0-9.]', '') AS response_case_resolved__c,
          CASE
              WHEN (case_reopen_link=''
                    OR length(case_reopen_link)=0
                    OR case_reopen_link IS NULL
                    OR case_reopen_link = 'nan') THEN regexp_replace(regexp_replace(json_extract(response_additional_detail__c, "$.response_reopen_case__c"), r'["]', ''), r'^nan$', '')
              ELSE case_reopen_link
          END AS response_reopen_case__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_support_rep_sat__c"), r'[^a-za-z0-9.]', '') AS response_support_rep_sat__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_communicating_clearly__c"), r'[^a-za-z0-9.]', '') AS response_communicating_clearly__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_additional_questions__c"), r'[^a-za-z0-9.]', '') AS response_additional_questions__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_online_resources_why__c"), r'[^a-za-z0-9 .]', '') AS response_online_resources_why__c,
          regexp_replace(regexp_replace(json_extract(response_additional_detail__c, "$.response_online_resources_experience__c"), r'["]', ''), r'^nan$', '') AS response_online_resources_experience__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_rma_experience__c"), r'[^a-za-z0-9.]', '') AS response_rma_experience__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_improve_rma_experience__c"), r'[^a-za-z0-9.]', '') AS response_improve_rma_experience__c,
          regexp_replace(json_extract(response_additional_detail__c, "$.response_addl_feedback__c"), r'[^a-za-z0-9.]', '') AS response_addl_feedback__c,
          regexp_replace(json_extract(response_otherdetails__c, "$.casenumber"), r'[^a-za-z0-9.]', '') AS casenumber,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.ownername"), r'["]', ''), r'^nan$', '') AS ownername,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.subject"), r'["]', ''), r'^nan$', '') AS subject,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.type"), r'["]', ''), r'^nan$', '') AS TYPE,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.caseorigin"), r'["]', ''), r'^nan$', '') AS caseorigin,
          regexp_replace(json_extract(response_otherdetails__c, "$.openeddate"), r'[^a-za-z0-9/]', '') AS openeddate,
          regexp_replace(json_extract(response_otherdetails__c, "$.closeddate"), r'[^a-za-z0-9/]', '') AS closeddate,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.contactname"), r'["]', ''), r'^nan$', '') AS contactname,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.contactemail"), r'["]', ''), r'^nan$', '') AS contactemail,
          regexp_replace(regexp_replace(json_extract(response_otherdetails__c, "$.phone"), r'^"nan"$', ''), r'["&                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.osrelease"),r'"]',''),r'^nan$','') as osrelease,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.product"),r'["]',''),r'^nan$','')  as product ,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.rma"),r'"]',''),r'^nan$','') rma,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.accountheatre"),r'["]',''),'nan','')  as accountheatre,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.billingcountry"),r'"]',''),'nan','')  as billingcountry,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.billingstate"),r'["]',''),'nan','')  as billingstate,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.accountname"),r'"]',''),r'^nan$','') as accountname,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.servicelevel"),r'["]',''),'nan','') as servicelevel,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.owneremail"),r'"]',''),r'^nan$','')  as owneremail,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ownerrole"),r'["]',''),r'^nan$','')  as ownerrole,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ownermanagername"),r'"]',''),r'^nan$','')  as ownermanagername,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ownermanageremail"),r'["]',''),r'^nan$','')  as ownermanageremail,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ownerlocation"),r'"]',''),r'^nan$','')  as ownerlocation,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.bugnumber"),r'(^"|"$)', ''),r'^nan$','')  as bugnumber,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.registeredcompany"),r'["]',''),r'^nan$','') as registeredcompany,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.enduserflag"),r'[^a-za-z0-9.]',''),r'^nan$','')  as enduserflag,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.asccasenumber"),r'"]',''),'nan','')  as asccasenumber,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ascflag"),r'[^a-za-z0-9.]',''),r'^nan$','')  as ascflag,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.registeredcompanyservicelevel"),r'["]',''),r'^nan$','')  as registeredcompanyservicelevel,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.accountservicelevel"),r'"]',''),'nan','')  as accountservicelevel,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.supportdeliverytype"),r'["]',''),'nan','')  as supportdeliverytype,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.designatedengineer"),r'"]',''),r'^nan$','')  as designatedengineer,                 replace(regexp_replace(json_extract(response_otherdetails__c,"$.supporttype"),r'["]',''),'nan','')  as supporttype,                 regexp_replace(json_extract(response_otherdetails__c,"$.accountid"),r'"]','') as accountid,                 regexp_replace(json_extract(response_otherdetails__c,"$.originalaccountid"),r'["]','') as originalaccountid,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.asccontactname"),r'"]',''),r'^nan$','') as asccontactname,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.asccontactemail"),r'["]',''),r'^nan$','') as asccontactemail,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ownermysaleslevel"),r'"]',''),r'^nan$','') as ownermysaleslevel,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.ownertheatre"),r'["]',''),r'^nan$','') as ownertheatre,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.partnerservicesprogrammanageremail"),r'"]',''),r'^nan$','') as partnerservicesprogrammanageremail,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.partnerservicesprogrammanager"),r'["]',''),r'^nan$','') as partnerservicesprogrammanager,                 regexp_replace(regexp_replace(json_extract(response_otherdetails__c,"$.caseid"),r' "]',''),r'^nan$','') as caseid,                 feedback_origin__c as surveytype                 from (                         select support_data.*, surveyticket_data.*,user_data.*                         from (                         select                         safe_cast(id as string) as id,record_medallia_id__c, response_date__c, lastmodifieddate,case_reopen_link,                         response_additional_detail__c, response_otherdetails__c, record_status__c, isdeleted,record_creation_date__c,createddate,feedback_origin__c,data_contact_name                         from `itp-aia-datalake.qualtrics_survey.customer_feedback__c`                         where feedback_origin__c in ('support','ASC support')                         ) support_data                         left outer join                         (                         select                         response_id__c,service_support_agent_email__c, record_ingestion_time, approved__c                         from `itp-aia-datalake.gcsd.sfdc_survey_ticket__c`                         where approved__c is true                         and service_support_agent_email__c is not null                         and service_support_agent_email__c != ''                         ) surveyticket_data                         on support_data.record_medallia_id__c = surveyticket_data.response_id__c                         left outer join                         (                         select name,username,usertype                         from `itp-aia-datalake.gcsd.sfdc_user`                         where usertype = "standard") user_data ON surveyticket_data.service_support_agent_email__c = user_data.username) )
"""


@pytest.fixture
def atlan_failed_query_entries_6() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="unknown",  # Could be the service account executing the query
            query=FAILED_ATLAN_RAHUL_QUERY_2,
            statementType="INSERT",
            project_id="itp-aia-datalake",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/qualtrics_survey/tables/customer_feedback__c"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/gcsd/tables/sfdc_survey_ticket__c"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/itp-aia-datalake/datasets/gcsd/tables/sfdc_user"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/gcsd_dm/tables/tb_gcs_qualtrics_support_survey"
            ),
        )
    ]

FAILED_ATLAN_RAHUL_QUERY_3 = """
insert into `itp-aia-datalake.revenue.tb_stg_dcwf_cols_1`  select df.*, t. fiscalyear,t. fiscal_qtr, t.fiscal_month,opp.ent_product_count__c,prod.renewal,prod.product_code
               ,if(parent_prod.product_code is null,null,parent_prod.product_code) as parent_product_code
                , if(parent_prod.standard_term is null or trim(parent_prod.standard_term) ='',df.standard_term
                     ,if((df.customer_order_type in ( 'zrcp', 'zrcb','zcpe') and  opportunity_closedate >= '2021-05-01') or df.is_steelbrick = 'y'
                          ,cast(date_diff(startend.end_date ,startend.start_date,day) as string),
                                                         parent_prod.standard_term) ) as parent_standard_term
               ,opp.name as opp_name ,prod.product_type1,prod.product_family
                , case when country__c in ('anguilla','antigua and barbuda','argentina','aruba','bahamas','barbados','belize','bermuda','bolivia','brazil','cayman islands','chile','colombia','costa rica','curacao','dominica',
                                           'dominican republic','ecuador','el salvador','grenada','guadeloupe','guatemala','guyana','haiti','honduras','jamaica','martinique','mexico','montserrat','netherlands antilles','nicaragua',
                                           'panama','paraguay','peru','puerto rico','saint kitts and nevis','saint lucia','saint martin','saint vincent and the grenadines','sint maarten','suriname','trinidad and tobago','turks and caicos islands',
                                           'uruguay','venezuela','virgin islands, british','virgin islands, u.s.') 
                                    then 'america' 
                       when country__c = 'japan' and theatre__c = 'japac' then 'jp'
                       when country__c != 'japan' and theatre__c = 'japac' then 'apac'
                       else ctry.theatre__c 
                  end as theatre
                ,concat( cast(t.fiscalyear as string), lpad(cast(t.fiscal_month as string),2,'0')) as period_end_date
                ,opp.m_a_source__c,opp.opportunity_owner_theatre__c as opportunity_owner_theatre
                ,opp_detail.opportunity_account_theatre__c as account_theater
                ,if (parent_prod.acv_compensation is null,prod.acv_compensation, parent_prod.acv_compensation) as acv_compensation
                ,prod.product_group4,prod.solution_type,prod.mgnbr_value as bbr_group
                from `itp-aia-datalake.revenue.tb_stg_dc_waterfall_ext_newstartend` df
                left outer join (select a.*,case when a.date <= '2022-01-31' then '2022-01-31' when a.date >= current_date then current_quarter_end_date else b.quarter_end_date end quarter_end_date
                                    from `itp-aia-datalake.it_dm.tb_pipe_fiscal_periods` a 
                                  left join (select fiscal_qtr_code, max(date) quarter_end_date from `itp-aia-datalake.it_dm.tb_pipe_fiscal_periods` group by fiscal_qtr_code) b
                                                                         on a.fiscal_qtr_code = b.fiscal_qtr_code
                                  cross join (select max(date) current_quarter_end_date from `itp-aia-datalake.it_dm.tb_pipe_fiscal_periods` 
                                             where fiscal_qtr_code = (select fiscal_qtr_code from `itp-aia-datalake.it_dm.tb_pipe_fiscal_periods` where date = current_date )) c   
                  )time on df.opportunity_closedate  = time.date
                left outer join `itp-aia-datalake.revenue.tb_stg_prodsnapshot` prod on df.material = prod.sap_material_number and time.quarter_end_date = prod.quarter_end_date
                left outer join `itp-aia-datalake.revenue.tb_stg_prodsnapshot` parent_prod on df.parent_material = parent_prod.sap_material_number and time.quarter_end_date = parent_prod.quarter_end_date
                left outer join (select id,closedate,ent_product_count__c,name ,m_a_source__c,opportunity_owner_theatre__c from `itp-aia-datalake.sfdc.opportunity` where stagename = "10 - closed - won" ) opp on substr(opp.id,1,15) = substr(df.opportunity_id,1,15)
                left outer join `itp-aia-datalake.it_dm.tb_pipe_fiscal_periods` t  on df.opportunity_closedate = t.date
                left outer join `itp-aia-datalake.sfdc.country__c` ctry on df.bill_to_country = ctry.iso_2_char__c
                left outer join `itp-aia-datalake.revenue.tb_stg_opp_account_theatre` opp_detail on df.opportunity_id = opp_detail.opportunity_id
                left outer join `itp-aia-datalake.revenue.tb_stg_dc_sap_startend_date` startend
                on df.order_num = startend.order_num and ifnull(df.parent_material,df.material) = startend.material
                where df.month_end_date = '2025-02-28'
"""

@pytest.fixture
def atlan_failed_query_entries_7() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="unknown",  # Replace with actual service account if known
            query=FAILED_ATLAN_RAHUL_QUERY_3,  # Replace with your actual query string variable
            statementType="INSERT",
            project_id="itp-aia-datalake",
            referencedViews=[
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/revenue/tables/tb_stg_dc_waterfall_ext_newstartend"),
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/it_dm/tables/tb_pipe_fiscal_periods"),
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/revenue/tables/tb_stg_prodsnapshot"),
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/sfdc/tables/opportunity"),
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/sfdc/tables/country__c"),
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/revenue/tables/tb_stg_opp_account_theatre"),
                BigQueryTableRef.from_string_name("projects/itp-aia-datalake/datasets/revenue/tables/tb_stg_dc_sap_startend_date"),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/itp-aia-datalake/datasets/revenue/tables/tb_stg_dcwf_cols_1"
            ),
        )
    ]

def test_broken_atlan_lineage(atlan_failed_query_entries_7: List[QueryEvent]) -> None:
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    # bq_table = BigQueryTableRef.from_string_name(
    #     "projects/itp-aia-datalake/datasets/asr_selfserve/tables/tb_asr_key_account_stakeholders"
    # )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        iter(atlan_failed_query_entries_7)
    )
    print(lineage_map)

    assert 1 == 2
    # upstream_lineage = extractor.get_lineage_for_table(
    #     bq_table=bq_table,
    #     bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,itp-aia-datalake.cortex.tb_tsm_xdr_usage,PROD)",
    #     lineage_metadata=lineage_map,
    # )
    # assert upstream_lineage
    # assert len(upstream_lineage.upstreams) == 7

def test_lineage_with_timestamps(lineage_entries: List[QueryEvent]) -> None:
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    bq_table = BigQueryTableRef.from_string_name(
        "projects/my_project/datasets/my_dataset/tables/my_table"
    )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        iter(lineage_entries)
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        lineage_metadata=lineage_map,
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 4


def test_column_level_lineage(lineage_entries: List[QueryEvent]) -> None:
    config = BigQueryV2Config(extract_column_lineage=True, incremental_lineage=False)
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    bq_table = BigQueryTableRef.from_string_name(
        "projects/my_project/datasets/my_dataset/tables/my_table"
    )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        lineage_entries[:1],
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        lineage_metadata=lineage_map,
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 2
    assert (
        upstream_lineage.fineGrainedLineages
        and len(upstream_lineage.fineGrainedLineages) == 2
    )


def test_lineage_for_external_bq_table(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="bq_gcs_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    def fake_schema_metadata(entity_urn: str) -> models.SchemaMetadataClass:
        return models.SchemaMetadataClass(
            schemaName="sample_schema",
            platform="urn:li:dataPlatform:gcs",  # important <- platform must be an urn
            version=0,
            hash="",
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="age",
                    type=models.SchemaFieldDataTypeClass(type=models.NumberTypeClass()),
                    nativeDataType="int",
                ),
                models.SchemaFieldClass(
                    fieldPath="firstname",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                ),
                models.SchemaFieldClass(
                    fieldPath="lastname",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                ),
            ],
        )

    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore
    path_specs: List[PathSpec] = [
        PathSpec(include="gs://bigquery_data/{table}/*.parquet"),
        PathSpec(include="gs://bigquery_data/customer3/{table}/*.parquet"),
    ]
    gcs_lineage_config: GcsLineageProviderConfig = GcsLineageProviderConfig(
        path_specs=path_specs
    )

    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=True,
        gcs_lineage_config=gcs_lineage_config,
    )

    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "gs://bigquery_data/customer1/*.parquet",
            "gs://bigquery_data/customer2/*.parquet",
            "gs://bigquery_data/customer3/my_table/*.parquet",
        ],
        graph=pipeline_context.graph,
    )

    expected_schema_field_urns = [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD),age)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD),firstname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD),lastname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD),age)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD),firstname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD),lastname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD),age)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD),firstname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD),lastname)",
    ]
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 3
    assert (
        upstream_lineage.fineGrainedLineages
        and len(upstream_lineage.fineGrainedLineages) == 9
    )
    # Extracting column URNs from upstream_lineage.upstreams
    actual_schema_field_urns = [
        fine_grained_lineage.upstreams[0]
        if fine_grained_lineage.upstreams is not None
        else []
        for fine_grained_lineage in upstream_lineage.fineGrainedLineages
    ]
    assert all(urn in expected_schema_field_urns for urn in actual_schema_field_urns), (
        "Some expected column URNs are missing from fine grained lineage."
    )


def test_lineage_for_external_bq_table_no_column_lineage(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="bq_gcs_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    def fake_schema_metadata(entity_urn: str) -> Optional[models.SchemaMetadataClass]:
        return None

    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore
    path_specs: List[PathSpec] = [
        PathSpec(include="gs://bigquery_data/{table}/*.parquet"),
        PathSpec(include="gs://bigquery_data/customer3/{table}/*.parquet"),
    ]
    gcs_lineage_config: GcsLineageProviderConfig = GcsLineageProviderConfig(
        path_specs=path_specs
    )

    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=True,
        gcs_lineage_config=gcs_lineage_config,
    )

    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "gs://bigquery_data/customer1/*.parquet",
            "gs://bigquery_data/customer2/*.parquet",
            "gs://bigquery_data/customer3/my_table/*.parquet",
        ],
        graph=pipeline_context.graph,
    )

    expected_dataset_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD)",
    ]
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 3
    # Extracting dataset URNs from upstream_lineage.upstreams
    actual_dataset_urns = [upstream.dataset for upstream in upstream_lineage.upstreams]
    assert all(urn in actual_dataset_urns for urn in expected_dataset_urns), (
        "Some expected dataset URNs are missing from upstream lineage."
    )
    assert upstream_lineage.fineGrainedLineages is None


def test_lineage_for_external_table_with_non_gcs_uri(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="non_gcs_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=False,  # Column lineage disabled for simplicity
    )
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "https://some_non_gcs_path/customer1/file.csv",
            "https://another_path/file.txt",
        ],
        graph=pipeline_context.graph,
    )

    assert upstream_lineage is None


def test_lineage_for_external_table_path_not_matching_specs(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="path_not_matching_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    path_specs: List[PathSpec] = [
        PathSpec(include="gs://different_data/db2/db3/{table}/*.parquet"),
    ]
    gcs_lineage_config: GcsLineageProviderConfig = GcsLineageProviderConfig(
        path_specs=path_specs, ignore_non_path_spec_path=True
    )
    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=False,
        gcs_lineage_config=gcs_lineage_config,
    )

    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "gs://bigquery_data/customer1/*.parquet",
            "gs://bigquery_data/customer2/*.parquet",
        ],
        graph=pipeline_context.graph,
    )

    assert upstream_lineage is None
