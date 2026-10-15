"""
silver.py  (AWS Glue Spark job; ADR-008)

Builds the silver (staging) tables from bronze with Spark SQL: renames to
clear snake_case names, casts types, turns placeholders into NULL, and keeps
the latest Care Compare release. PBJ keeps every quarter (a time series).
Conventions match the Snowflake project's dbt staging models.

Lean scope: the five tables the dashboard needs. The other bronze tables
remain queryable in Athena (cms_bronze) for future work.

Job arguments: --lake_root (s3://bucket)
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse_utils import get_args, snake, spark_session, write_table

args = get_args(["lake_root"])
root = args["lake_root"].rstrip("/")
spark = spark_session("silver")


def expr(src, kind):
    c = f"`{snake(src)}`"
    t = f"nullif(trim({c}), '')"
    if kind == "ccn":
        return f"lpad(trim({c}), 6, '0')"
    if kind == "text":
        return t
    if kind == "phone":
        return f"nullif({t}, '0000001111')"
    if kind == "county":
        return f"nullif({t}, '000')"
    if kind == "int":
        return f"try_cast({t} as int)"
    if kind == "bool":
        return f"try_cast({t} as boolean)"
    if kind == "date":
        return f"coalesce(to_date({t}, 'yyyy-MM-dd'), to_date({t}, 'MM/dd/yyyy'), to_date({t}, 'yyyyMMdd'))"
    if kind.startswith("dec("):
        p, s = kind[4:-1].split(",")
        return f"try_cast({t} as decimal({p},{s}))"
    if kind == "drop":
        return None
    raise ValueError(kind)


def build(table, spec, extra=(), latest_release_only=True, dedupe_on=None):
    select = [f"{expr(s, k)} as {d}" for s, d, k in spec if k != "drop"]
    select += [f"{e} as {d}" for e, d in extra]
    select += ["release", "_source_file as source_file", "_ingested_at as loaded_at"]
    where = (f"where release = (select max(release) from cms_bronze.{table})" if latest_release_only else "")
    sql = f"select\n  " + ",\n  ".join(select) + f"\nfrom cms_bronze.{table}\n{where}"
    df = spark.sql(sql)
    if dedupe_on:
        # the same file can land under several releases; keep the newest copy of each row
        w = Window.partitionBy(dedupe_on).orderBy(F.col("release").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    rows = write_table(spark, df, "cms_silver", table, f"{root}/silver/{table}/")
    if rows == 0:
        raise RuntimeError(f"silver {table}: no rows")
    return df


# --- nh_provider_info: one row per facility --------------------------------------
PROVIDER = [
    ('CMS Certification Number (CCN)', 'ccn', 'ccn'),
    ('Provider Name', 'provider_name', 'text'),
    ('Provider Address', 'address', 'text'),
    ('City/Town', 'city', 'text'),
    ('State', 'state', 'text'),
    ('ZIP Code', 'zip_code', 'text'),
    ('Telephone Number', 'phone', 'phone'),
    ('Provider SSA County Code', 'ssa_county_code', 'county'),
    ('County/Parish', 'county', 'text'),
    ('Ownership Type', 'ownership_type', 'text'),
    ('Number of Certified Beds', 'certified_beds', 'int'),
    ('Average Number of Residents per Day', 'avg_residents_per_day', 'dec(10,2)'),
    ('Average Number of Residents per Day Footnote', 'avg_residents_per_day_footnote', 'text'),
    ('Provider Type', 'provider_type', 'text'),
    ('Provider Resides in Hospital', 'is_in_hospital', 'bool'),
    ('Legal Business Name', 'legal_business_name', 'text'),
    ('Date First Approved to Provide Medicare and Medicaid Services', 'medicare_medicaid_approval_date', 'date'),
    ('Affiliated Entity Name', 'affiliated_entity_name', 'text'),
    ('Affiliated Entity ID', 'affiliated_entity_id', 'text'),
    ('Continuing Care Retirement Community', 'is_ccrc', 'bool'),
    ('Special Focus Status', 'special_focus_status', 'text'),
    ('Abuse Icon', 'has_abuse_icon', 'bool'),
    ('Most Recent Health Inspection More Than 2 Years Ago', 'is_last_inspection_over_2_years', 'bool'),
    ('Provider Changed Ownership in Last 12 Months', 'changed_ownership_last_12_months', 'bool'),
    ('With a Resident and Family Council', 'resident_family_council', 'text'),
    ('Automatic Sprinkler Systems in All Required Areas', 'sprinkler_systems_status', 'text'),
    ('Overall Rating', 'overall_rating', 'int'),
    ('Overall Rating Footnote', 'overall_rating_footnote', 'text'),
    ('Health Inspection Rating', 'health_inspection_rating', 'int'),
    ('Health Inspection Rating Footnote', 'health_inspection_rating_footnote', 'text'),
    ('QM Rating', 'qm_rating', 'int'),
    ('QM Rating Footnote', 'qm_rating_footnote', 'text'),
    ('Long-Stay QM Rating', 'long_stay_qm_rating', 'int'),
    ('Long-Stay QM Rating Footnote', 'long_stay_qm_rating_footnote', 'text'),
    ('Short-Stay QM Rating', 'short_stay_qm_rating', 'int'),
    ('Short-Stay QM Rating Footnote', 'short_stay_qm_rating_footnote', 'text'),
    ('Staffing Rating', 'staffing_rating', 'int'),
    ('Staffing Rating Footnote', 'staffing_rating_footnote', 'text'),
    ('Reported Staffing Footnote', 'reported_staffing_footnote', 'text'),
    ('Physical Therapist Staffing Footnote', 'pt_staffing_footnote', 'text'),
    ('Reported Nurse Aide Staffing Hours per Resident per Day', 'reported_nurse_aide_hprd', 'dec(10,5)'),
    ('Reported LPN Staffing Hours per Resident per Day', 'reported_lpn_hprd', 'dec(10,5)'),
    ('Reported RN Staffing Hours per Resident per Day', 'reported_rn_hprd', 'dec(10,5)'),
    ('Reported Licensed Staffing Hours per Resident per Day', 'reported_licensed_hprd', 'dec(10,5)'),
    ('Reported Total Nurse Staffing Hours per Resident per Day', 'reported_total_nurse_hprd', 'dec(10,5)'),
    ('Total number of nurse staff hours per resident per day on the weekend', 'weekend_total_nurse_hprd', 'dec(10,5)'),
    ('Registered Nurse hours per resident per day on the weekend', 'weekend_rn_hprd', 'dec(10,5)'),
    ('Reported Physical Therapist Staffing Hours per Resident Per Day', 'reported_pt_hprd', 'dec(10,5)'),
    ('Total nursing staff turnover', 'total_nursing_turnover_pct', 'dec(9,4)'),
    ('Total nursing staff turnover footnote', 'total_nursing_turnover_footnote', 'text'),
    ('Registered Nurse turnover', 'rn_turnover_pct', 'dec(9,4)'),
    ('Registered Nurse turnover footnote', 'rn_turnover_footnote', 'text'),
    ('Number of administrators who have left the nursing home', 'administrators_departed', 'int'),
    ('Administrator turnover footnote', 'administrator_turnover_footnote', 'text'),
    ('Nursing Case-Mix Index', 'nursing_case_mix_index', 'dec(10,5)'),
    ('Nursing Case-Mix Index Ratio', 'nursing_case_mix_index_ratio', 'dec(10,5)'),
    ('Case-Mix Nurse Aide Staffing Hours per Resident per Day', 'case_mix_nurse_aide_hprd', 'dec(10,5)'),
    ('Case-Mix LPN Staffing Hours per Resident per Day', 'case_mix_lpn_hprd', 'dec(10,5)'),
    ('Case-Mix RN Staffing Hours per Resident per Day', 'case_mix_rn_hprd', 'dec(10,5)'),
    ('Case-Mix Total Nurse Staffing Hours per Resident per Day', 'case_mix_total_nurse_hprd', 'dec(10,5)'),
    ('Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day', 'case_mix_weekend_total_nurse_hprd', 'dec(10,5)'),
    ('Adjusted Nurse Aide Staffing Hours per Resident per Day', 'adjusted_nurse_aide_hprd', 'dec(10,5)'),
    ('Adjusted LPN Staffing Hours per Resident per Day', 'adjusted_lpn_hprd', 'dec(10,5)'),
    ('Adjusted RN Staffing Hours per Resident per Day', 'adjusted_rn_hprd', 'dec(10,5)'),
    ('Adjusted Total Nurse Staffing Hours per Resident per Day', 'adjusted_total_nurse_hprd', 'dec(10,5)'),
    ('Adjusted Weekend Total Nurse Staffing Hours per Resident per Day', 'adjusted_weekend_total_nurse_hprd', 'dec(10,5)'),
    ('Rating Cycle 1 Standard Survey Health Date', 'cycle_1_survey_date', 'date'),
    ('Rating Cycle 1 Total Number of Health Deficiencies', 'cycle_1_total_health_deficiencies', 'int'),
    ('Rating Cycle 1 Number of Standard Health Deficiencies', 'cycle_1_standard_health_deficiencies', 'int'),
    ('Rating Cycle 1 Number of Complaint Health Deficiencies', 'cycle_1_complaint_health_deficiencies', 'int'),
    ('Rating Cycle 1 Health Deficiency Score', 'cycle_1_health_deficiency_score', 'dec(12,4)'),
    ('Rating Cycle 1 Number of Health Revisits', 'cycle_1_health_revisits', 'int'),
    ('Rating Cycle 1 Health Revisit Score', 'cycle_1_health_revisit_score', 'dec(12,4)'),
    ('Rating Cycle 1 Total Health Score', 'cycle_1_total_health_score', 'dec(12,4)'),
    ('Rating Cycle 2 Standard Health Survey Date', 'cycle_2_survey_date', 'date'),
    ('Rating Cycle 2 Total Number of Health Deficiencies', 'cycle_2_total_health_deficiencies', 'int'),
    ('Rating Cycle 2 Number of Standard Health Deficiencies', 'cycle_2_standard_health_deficiencies', 'int'),
    ('Rating Cycle 2 Number of Complaint Health Deficiencies', 'cycle_2_complaint_health_deficiencies', 'int'),
    ('Rating Cycle 2 Health Deficiency Score', 'cycle_2_health_deficiency_score', 'dec(12,4)'),
    ('Rating Cycle 2 Number of Health Revisits', 'cycle_2_health_revisits', 'int'),
    ('Rating Cycle 2 Health Revisit Score', 'cycle_2_health_revisit_score', 'dec(12,4)'),
    ('Rating Cycle 2 Total Health Score', 'cycle_2_total_health_score', 'dec(12,4)'),
    ('Rating Cycle 3 Standard Health Survey Date', 'cycle_3_survey_date', 'date'),
    ('Rating Cycle 3 Total Number of Health Deficiencies', 'cycle_3_total_health_deficiencies', 'int'),
    ('Rating Cycle 3 Number of Standard Health Deficiencies', 'cycle_3_standard_health_deficiencies', 'int'),
    ('Rating Cycle 3 Number of Complaint Health Deficiencies', 'cycle_3_complaint_health_deficiencies', 'int'),
    ('Rating Cycle 3 Health Deficiency Score', 'cycle_3_health_deficiency_score', 'dec(12,4)'),
    ('Rating Cycle 3 Number of Health Revisits', 'cycle_3_health_revisits', 'int'),
    ('Rating Cycle 3 Health Revisit Score', 'cycle_3_health_revisit_score', 'dec(12,4)'),
    ('Rating Cycle 3 Total Health Score', 'cycle_3_total_health_score', 'dec(12,4)'),
    ('Total Weighted Health Survey Score', 'total_weighted_health_survey_score', 'dec(12,4)'),
    ('Number of Facility Reported Incidents', 'facility_reported_incidents', 'int'),
    ('Number of Substantiated Complaints', 'substantiated_complaints', 'int'),
    ('Number of Citations from Infection Control Inspections', 'infection_control_citations', 'int'),
    ('Number of Fines', 'fines_count', 'int'),
    ('Total Amount of Fines in Dollars', 'fines_total_usd', 'dec(14,2)'),
    ('Number of Payment Denials', 'payment_denials_count', 'int'),
    ('Total Number of Penalties', 'penalties_total_count', 'int'),
    ('Location', 'location', 'drop'),
    ('Latitude', 'latitude', 'dec(10,6)'),
    ('Longitude', 'longitude', 'dec(10,6)'),
    ('Geocoding Footnote', 'geocoding_footnote', 'text'),
    ('Processing Date', 'processing_date', 'date')
]
build("nh_provider_info", PROVIDER)
dups = spark.sql("select ccn from cms_silver.nh_provider_info group by ccn having count(*) > 1").count()
if dups:
    raise RuntimeError(f"silver nh_provider_info: {dups} duplicate ccn values")

# --- quality measures (MDS): one row per facility per measure ---------------------
MDS = [
    ("CMS Certification Number (CCN)", "ccn", "ccn"),
    ("Measure Code", "measure_code", "text"),
    ("Measure Description", "measure_description", "text"),
    ("Resident type", "resident_type", "text"),
    ("Q1 Measure Score", "q1_score", "dec(14,6)"),
    ("Footnote for Q1 Measure Score", "q1_score_footnote", "text"),
    ("Q2 Measure Score", "q2_score", "dec(14,6)"),
    ("Footnote for Q2 Measure Score", "q2_score_footnote", "text"),
    ("Q3 Measure Score", "q3_score", "dec(14,6)"),
    ("Footnote for Q3 Measure Score", "q3_score_footnote", "text"),
    ("Q4 Measure Score", "q4_score", "dec(14,6)"),
    ("Footnote for Q4 Measure Score", "q4_score_footnote", "text"),
    ("Four Quarter Average Score", "four_quarter_avg_score", "dec(14,6)"),
    ("Footnote for Four Quarter Average Score", "four_quarter_avg_footnote", "text"),
    ("Used in Quality Measure Five Star Rating", "is_used_in_five_star", "bool"),
    ("Measure Period", "measure_period", "text"),
]
KEY = [("lpad(trim(`cms_certification_number_ccn`), 6, '0') || '-' || trim(`measure_code`)", "facility_measure_key")]
build("nh_quality_measures_mds", MDS, KEY)

# --- quality measures (claims): one row per facility per measure ------------------
CLAIMS = [
    ("CMS Certification Number (CCN)", "ccn", "ccn"),
    ("Measure Code", "measure_code", "text"),
    ("Measure Description", "measure_description", "text"),
    ("Resident type", "resident_type", "text"),
    ("Adjusted Score", "adjusted_score", "dec(14,6)"),
    ("Observed Score", "observed_score", "dec(14,6)"),
    ("Expected Score", "expected_score", "dec(14,6)"),
    ("Footnote for Score", "score_footnote", "text"),
    ("Used in Quality Measure Five Star Rating", "is_used_in_five_star", "bool"),
    ("Measure Period", "measure_period", "text"),
]
build("nh_quality_measures_claims", CLAIMS, KEY)

# --- SNF VBP: one row per scored facility (feeds the Quality of Care page later) ----
VBP = [
    ("CMS Certification Number (CCN)", "ccn", "ccn"),
    ("SNF VBP Program Ranking", "vbp_ranking", "int"),
    ("Footnote -- SNF VBP Program Ranking", "vbp_ranking_footnote", "text"),
    ("Baseline Period: FY 2019 Risk-Standardized Readmission Rate", "baseline_readmission_rate", "dec(14,8)"),
    ("Performance Period: FY 2022 Risk-Standardized Readmission Rate", "performance_readmission_rate", "dec(14,8)"),
    ("Achievement Score", "achievement_score", "dec(14,6)"),
    ("Improvement Score", "improvement_score", "dec(14,6)"),
    ("Performance Score", "performance_score", "dec(14,6)"),
    ("Incentive Payment Multiplier", "incentive_payment_multiplier", "dec(16,12)"),
]
build("snf_vbp_facility_performance", VBP)

# --- PBJ daily nurse staffing: one row per facility per day, all quarters -----------
PBJ = [("PROVNUM", "ccn", "ccn"), ("WorkDate", "work_date", "date"), ("CY_Qtr", "calendar_quarter", "text"),
       ("MDScensus", "resident_census", "int")]
for src, dst in (("RNDON", "rn_don"), ("RNadmin", "rn_admin"), ("RN", "rn"), ("LPNadmin", "lpn_admin"),
                 ("LPN", "lpn"), ("CNA", "cna"), ("NAtrn", "na_trainee"), ("MedAide", "med_aide")):
    for suf, dsuf in (("", "hours"), ("_emp", "emp_hours"), ("_ctr", "ctr_hours")):
        PBJ.append((f"Hrs_{src}{suf}", f"{dst}_{dsuf}", "dec(12,2)"))
build("pbj_daily_nurse_staffing", PBJ,
      [("lpad(trim(`provnum`), 6, '0') || '-' || trim(`workdate`)", "facility_day_key")],
      latest_release_only=False, dedupe_on="facility_day_key")
dups = spark.sql("select facility_day_key from cms_silver.pbj_daily_nurse_staffing group by 1 having count(*) > 1").count()
if dups:
    raise RuntimeError(f"silver pbj_daily_nurse_staffing: {dups} duplicate facility-days")
