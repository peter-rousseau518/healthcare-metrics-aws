"""
gold.py  (AWS Glue Spark job; ADR-008)

Builds the gold marts from silver with Spark SQL. Metric definitions match
the Snowflake project's ADR-024: HPRD = total hours / total resident-days;
RN = DON + RN admin + RN; LPN = LPN admin + LPN; licensed = RN + LPN;
occupancy = average daily census / certified beds; valid facilities have
>= 60 reported days, occupancy 0-125%, total HPRD 0.5-12.

Each mart is written as Parquet (metrics as double) and registered in
cms_gold for Athena, then exposed as a temp view for marts built after it.

Job arguments: --lake_root (s3://bucket)
"""
from lakehouse_utils import get_args, spark_session, write_table

args = get_args(["lake_root"])
root = args["lake_root"].rstrip("/")
spark = spark_session("gold")

MARTS = {}

MARTS["dim_facility"] = """
select
    ccn, provider_name, city, state, county, zip_code,
    cast(latitude as double) as latitude, cast(longitude as double) as longitude,
    ownership_type,
    case
        when ownership_type ilike 'For profit%' then 'For profit'
        when ownership_type ilike 'Non profit%' then 'Nonprofit'
        when ownership_type ilike 'Government%' then 'Government'
        else 'Other'
    end as ownership_group,
    coalesce(provider_type, 'Unknown') as provider_type,
    case when is_in_hospital then 'Hospital-based'
         when not is_in_hospital then 'Freestanding'
         else 'Unknown' end as facility_setting,
    certified_beds,
    case when certified_beds < 50 then '1-49'
         when certified_beds < 100 then '50-99'
         when certified_beds < 150 then '100-149'
         when certified_beds < 200 then '150-199'
         else '200+' end as bed_size_band,
    affiliated_entity_name,
    affiliated_entity_name is not null as is_chain_affiliated,
    overall_rating, health_inspection_rating, qm_rating, staffing_rating,
    avg_residents_per_day,
    avg_residents_per_day / nullif(certified_beds, 0) as care_compare_occupancy_rate,
    total_nursing_turnover_pct, rn_turnover_pct,
    substantiated_complaints, facility_reported_incidents,
    fines_total_usd, penalties_total_count,
    release
from cms_silver.nh_provider_info
"""

MARTS["fct_daily_staffing"] = """
with hours as (
    select
        facility_day_key, ccn, work_date, calendar_quarter, resident_census,
        coalesce(rn_don_hours, 0) + coalesce(rn_admin_hours, 0) + coalesce(rn_hours, 0)                as rn_hours,
        coalesce(rn_don_ctr_hours, 0) + coalesce(rn_admin_ctr_hours, 0) + coalesce(rn_ctr_hours, 0)    as rn_contract_hours,
        coalesce(lpn_admin_hours, 0) + coalesce(lpn_hours, 0)                                         as lpn_hours,
        coalesce(lpn_admin_ctr_hours, 0) + coalesce(lpn_ctr_hours, 0)                                 as lpn_contract_hours,
        coalesce(cna_hours, 0) + coalesce(na_trainee_hours, 0) + coalesce(med_aide_hours, 0)          as nurse_aide_hours,
        coalesce(cna_ctr_hours, 0) + coalesce(na_trainee_ctr_hours, 0) + coalesce(med_aide_ctr_hours, 0) as nurse_aide_contract_hours
    from cms_silver.pbj_daily_nurse_staffing
)
select
    h.facility_day_key, h.ccn, h.work_date, h.calendar_quarter,
    dayofweek(h.work_date) in (1, 7)                         as is_weekend,
    h.resident_census, f.certified_beds,
    h.resident_census / nullif(f.certified_beds, 0)          as occupancy_rate,
    h.rn_hours, h.rn_contract_hours, h.lpn_hours, h.lpn_contract_hours,
    h.rn_hours + h.lpn_hours                                 as licensed_nurse_hours,
    h.rn_contract_hours + h.lpn_contract_hours               as licensed_nurse_contract_hours,
    h.nurse_aide_hours, h.nurse_aide_contract_hours,
    h.rn_hours + h.lpn_hours + h.nurse_aide_hours            as total_nurse_hours
from hours h
inner join dim_facility f on f.ccn = h.ccn
"""

MARTS["mart_facility_staffing"] = """
with agg as (
    select
        ccn, calendar_quarter,
        count(*)                                              as days_reported,
        min(work_date)                                        as period_start,
        max(work_date)                                        as period_end,
        avg(resident_census)                                  as avg_daily_census,
        sum(resident_census)                                  as resident_days,
        sum(rn_hours)                                         as rn_hours,
        sum(lpn_hours)                                        as lpn_hours,
        sum(licensed_nurse_hours)                             as licensed_nurse_hours,
        sum(licensed_nurse_contract_hours)                    as licensed_nurse_contract_hours,
        sum(nurse_aide_hours)                                 as nurse_aide_hours,
        sum(total_nurse_hours)                                as total_nurse_hours,
        sum(if(is_weekend, licensed_nurse_hours, 0))          as weekend_licensed_hours,
        sum(if(is_weekend, resident_census, 0))               as weekend_resident_days,
        sum(if(not is_weekend, licensed_nurse_hours, 0))      as weekday_licensed_hours,
        sum(if(not is_weekend, resident_census, 0))           as weekday_resident_days
    from fct_daily_staffing
    group by ccn, calendar_quarter
),
metrics as (
    select
        a.ccn, a.calendar_quarter, a.days_reported, a.period_start, a.period_end,
        a.avg_daily_census, f.certified_beds,
        a.avg_daily_census / nullif(f.certified_beds, 0)                      as occupancy_rate,
        a.rn_hours / nullif(a.resident_days, 0)                               as rn_hprd,
        a.lpn_hours / nullif(a.resident_days, 0)                              as lpn_hprd,
        a.licensed_nurse_hours / nullif(a.resident_days, 0)                   as licensed_nurse_hprd,
        a.nurse_aide_hours / nullif(a.resident_days, 0)                       as nurse_aide_hprd,
        a.total_nurse_hours / nullif(a.resident_days, 0)                      as total_nurse_hprd,
        a.licensed_nurse_hours / a.days_reported * 7                          as licensed_nurse_hours_per_week,
        a.licensed_nurse_hours / a.days_reported * 7 / 40                     as licensed_nurse_fte_equivalent,
        a.licensed_nurse_contract_hours / a.days_reported * 7                 as contract_licensed_hours_per_week,
        a.licensed_nurse_contract_hours / nullif(a.licensed_nurse_hours, 0)   as contract_licensed_share,
        a.weekday_licensed_hours / nullif(a.weekday_resident_days, 0)         as weekday_licensed_hprd,
        a.weekend_licensed_hours / nullif(a.weekend_resident_days, 0)         as weekend_licensed_hprd,
        1 - (a.weekend_licensed_hours / nullif(a.weekend_resident_days, 0))
          / nullif(a.weekday_licensed_hours / nullif(a.weekday_resident_days, 0), 0) as weekend_licensed_drop_pct,
        f.total_nursing_turnover_pct, f.rn_turnover_pct,
        f.provider_name, f.state, f.ownership_group, f.provider_type, f.facility_setting,
        f.is_chain_affiliated, f.overall_rating, f.staffing_rating
    from agg a
    inner join dim_facility f on f.ccn = a.ccn
),
flagged as (
    select *,
        coalesce(days_reported >= 60
                 and occupancy_rate > 0 and occupancy_rate <= 1.25
                 and total_nurse_hprd between 0.5 and 12, false) as is_valid_for_analysis
    from metrics
)
select *,
    ccn || '-' || calendar_quarter as facility_quarter_key,
    if(is_valid_for_analysis,
       rank() over (partition by calendar_quarter, is_valid_for_analysis
                    order by contract_licensed_hours_per_week desc nulls last),
       null) as contract_licensed_hours_rank
from flagged
"""

MARTS["mart_staffing_occupancy_correlation"] = """
select
    calendar_quarter,
    coalesce(state, 'All states')                       as state,
    count(*)                                            as facility_count,
    avg(occupancy_rate)                                 as avg_occupancy_rate,
    avg(licensed_nurse_hprd)                            as avg_licensed_nurse_hprd,
    avg(total_nurse_hprd)                               as avg_total_nurse_hprd,
    corr(occupancy_rate, licensed_nurse_hprd)           as corr_occupancy_licensed_hprd,
    corr(occupancy_rate, rn_hprd)                       as corr_occupancy_rn_hprd,
    corr(occupancy_rate, total_nurse_hprd)              as corr_occupancy_total_hprd,
    calendar_quarter || '|' || coalesce(state, 'All states') as correlation_key
from mart_facility_staffing
where is_valid_for_analysis
group by grouping sets ((calendar_quarter, state), (calendar_quarter))
"""

MARTS["mart_staffing_by_state_and_type"] = """
with base as (
    select * from mart_facility_staffing where is_valid_for_analysis
),
long as (
    select calendar_quarter, state, 'Ownership type' as facility_type_dimension, ownership_group as facility_type,
           rn_hprd, lpn_hprd, licensed_nurse_hprd, total_nurse_hprd, occupancy_rate from base
    union all
    select calendar_quarter, state, 'Provider type', provider_type,
           rn_hprd, lpn_hprd, licensed_nurse_hprd, total_nurse_hprd, occupancy_rate from base
    union all
    select calendar_quarter, state, 'Facility setting', facility_setting,
           rn_hprd, lpn_hprd, licensed_nurse_hprd, total_nurse_hprd, occupancy_rate from base
)
select
    calendar_quarter,
    coalesce(state, 'All states')                       as state,
    facility_type_dimension, facility_type,
    count(*)                                            as facility_count,
    avg(rn_hprd)                                        as avg_rn_hprd,
    avg(lpn_hprd)                                       as avg_lpn_hprd,
    avg(licensed_nurse_hprd)                            as avg_licensed_nurse_hprd,
    avg(total_nurse_hprd)                               as avg_total_nurse_hprd,
    avg(occupancy_rate)                                 as avg_occupancy_rate,
    calendar_quarter || '|' || coalesce(state, 'All states') || '|'
        || facility_type_dimension || '|' || facility_type as state_type_key
from long
group by grouping sets (
    (calendar_quarter, state, facility_type_dimension, facility_type),
    (calendar_quarter, facility_type_dimension, facility_type)
)
"""

MARTS["mart_daily_staffing_trend"] = """
with d as (
    select daily.*, f.state
    from fct_daily_staffing daily
    inner join dim_facility f on f.ccn = daily.ccn
    where daily.resident_census > 0
)
select
    work_date,
    coalesce(state, 'All states')                                         as state,
    max(cast(is_weekend as int)) = 1                                      as is_weekend,
    count(*)                                                              as facilities_reporting,
    sum(resident_census)                                                  as total_residents,
    sum(resident_census) / nullif(sum(certified_beds), 0)                 as occupancy_rate,
    sum(licensed_nurse_hours) / nullif(sum(resident_census), 0)           as licensed_nurse_hprd,
    sum(total_nurse_hours) / nullif(sum(resident_census), 0)              as total_nurse_hprd,
    sum(licensed_nurse_contract_hours) / nullif(sum(licensed_nurse_hours), 0) as contract_licensed_share,
    cast(work_date as string) || '|' || coalesce(state, 'All states')     as date_state_key
from d
group by grouping sets ((work_date, state), (work_date))
"""

MARTS["dim_measure"] = """
with all_measures as (
    select measure_code, measure_description, resident_type, is_used_in_five_star, 'MDS' as measure_source
    from cms_silver.nh_quality_measures_mds
    union all
    select measure_code, measure_description, resident_type, is_used_in_five_star, 'Claims' as measure_source
    from cms_silver.nh_quality_measures_claims
)
select
    measure_code,
    first(measure_description, true)                   as measure_description,
    first(resident_type, true)                         as resident_type,
    first(measure_source, true)                        as measure_source,
    bool_or(coalesce(is_used_in_five_star, false))     as is_used_in_five_star
from all_measures
group by measure_code
"""

MARTS["fct_quality_measure_quarters"] = """
with unpivoted as (
    select ccn, measure_code, resident_type, measure_period, facility_measure_key, 1 as quarter_number, q1_score as score, q1_score_footnote as score_footnote from cms_silver.nh_quality_measures_mds
    union all
    select ccn, measure_code, resident_type, measure_period, facility_measure_key, 2, q2_score, q2_score_footnote from cms_silver.nh_quality_measures_mds
    union all
    select ccn, measure_code, resident_type, measure_period, facility_measure_key, 3, q3_score, q3_score_footnote from cms_silver.nh_quality_measures_mds
    union all
    select ccn, measure_code, resident_type, measure_period, facility_measure_key, 4, q4_score, q4_score_footnote from cms_silver.nh_quality_measures_mds
)
select
    facility_measure_key || '-Q' || quarter_number          as facility_measure_quarter_key,
    ccn, measure_code, resident_type, measure_period, quarter_number,
    add_months(to_date(substring_index(measure_period, '-', 1), 'yyyyMMdd'), 3 * (quarter_number - 1)) as quarter_start,
    score, score_footnote
from unpivoted
"""

MARTS["mart_stay_type_quality_trend"] = """
select
    m.resident_type, q.measure_code, m.measure_description, q.quarter_number,
    min(q.quarter_start)                    as quarter_start,
    count(*)                                as facility_count,
    avg(q.score)                            as avg_score,
    percentile_approx(q.score, 0.5)         as median_score,
    q.measure_code || '-Q' || q.quarter_number as measure_quarter_key
from fct_quality_measure_quarters q
inner join dim_measure m on m.measure_code = q.measure_code
where q.score is not null
group by m.resident_type, q.measure_code, m.measure_description, q.quarter_number
"""

KEYS = {
    "dim_facility": "ccn", "fct_daily_staffing": "facility_day_key", "mart_facility_staffing": "facility_quarter_key",
    "mart_staffing_occupancy_correlation": "correlation_key", "mart_staffing_by_state_and_type": "state_type_key",
    "mart_daily_staffing_trend": "date_state_key", "dim_measure": "measure_code",
    "fct_quality_measure_quarters": "facility_measure_quarter_key", "mart_stay_type_quality_trend": "measure_quarter_key",
}

for name, sql in MARTS.items():
    df = spark.sql(sql)
    rows = write_table(spark, df, "cms_gold", name, f"{root}/gold/{name}/", decimals_to_double=True)
    if rows == 0:
        raise RuntimeError(f"gold {name}: no rows")
    key = KEYS[name]
    dups = spark.sql(f"select {key} from cms_gold.{name} group by {key} having count(*) > 1").count()
    nulls = spark.sql(f"select count(*) c from cms_gold.{name} where {key} is null").first()["c"]
    if dups or nulls:
        raise RuntimeError(f"gold {name}: key {key} has {dups} duplicates and {nulls} nulls")
    spark.table(f"cms_gold.{name}").createOrReplaceTempView(name)
