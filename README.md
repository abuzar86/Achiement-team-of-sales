# achievement-team-of-sales

WITH 
date_day AS (

    SELECT DISTINCT date_day
    FROM dbt_prod.date_spine_day
    WHERE {{Date}} 
    AND date_day >= '2024-04-01'
),    

baseline_order AS 
(

    SELECT
        payment_at,
        user_id,
        gross_gmv
    from dbt_prod.REX_PRODUCT_ORDER_DETAIL
    WHERE 1=1
    [[
        AND
        CASE 
            WHEN {{Performance_filter}} = 'Regular Physical' THEN is_reguler_physical = 1 -- Handling Reseller Project Only Regular
            WHEN {{Performance_filter}} = 'Regular Blended' THEN is_reguler_blended = 1 -- Handling Reseller Project Only Regular
            WHEN {{Performance_filter}} = 'Physical All' THEN IS_RULE_CBP_ORGANIC = 1
            WHEN {{Performance_filter}} = 'Digital All' THEN IS_RULE_DIGITAL_SERVICE_ORGANIC = 1
            WHEN {{Performance_filter}} = 'Corporate Blended' THEN (IS_RULE_DIGITAL_SERVICE_ORGANIC = 1 or IS_RULE_CBP_ORGANIC = 1)
        END
    ]] 
    [[AND NOT {{category_2_exclusion}}]]
    [[AND NOT {{cluster_category_exclusion}}]]
    
),

summary_order_filtered AS 
(
   
    SELECT 
        user_id,
        SUM(gross_gmv) as total_gmv
    FROM baseline_order 
    WHERE 
        DATE(payment_at) IN (SELECT DISTINCT date_day FROM date_day)
    GROUP BY 1
),

summary_order_previous AS 
(
   
    SELECT  
        user_id,
        SUM(gross_gmv) as total_gmv
    FROM baseline_order 
    WHERE 
        DATE(payment_at) IN (SELECT DISTINCT DATEADD(month, -1, date_day) FROM date_day)
    GROUP BY 1
),

target_ar AS 
(
    
    SELECT 
        month_at,
        pic_name,
        target as target_ar
    FROM dbt_prod.DIM_SBU_APPSMITH_TARGET
    WHERE metrics = 'AR Retention'
    AND month_at = month(SELECT DISTINCT MIN(date_day) FROM date_day)
    AND year_at = year(SELECT MIN(date_day) FROM date_day)
),

target_gmv AS 
(
   
    SELECT 
        month_at,
        pic_name,
        target as target_gmv
    FROM dbt_prod.DIM_SBU_APPSMITH_TARGET
    WHERE metrics = 'GMV Retention'
    AND month_at = month(SELECT DISTINCT MIN(date_day) FROM date_day)
    AND year_at = year(SELECT MIN(date_day) FROM date_day)
 ),

join_group as 
(
    
    SELECT 
        user_id, 
        group_name as wag_name,
        CASE
            WHEN left_at IS NOT NULL THEN 'Left'
            ELSE 'Join'
        END AS current_status
    FROM dbt_prod.fct_wa_group_participant
    WHERE 
        group_name ILIKE ANY ('TBB%', 'KBB%')
        AND JOINED_AT <= (SELECT MAX(date_day) from date_day)
        AND (left_at IS NULL OR left_at <= (SELECT MAX(date_day) from date_day))
    GROUP BY ALL 
),


user_join_kbb_project AS
(
    
    SELECT
        DISTINCT
        month_at,
        province_city_kecamatan
    FROM dbt_prod.seed_kbb_area_project sk
    WHERE 
        month_at IN ((SELECT DISTINCT date_trunc('month', date_day) FROM date_day), (SELECT DISTINCT date_trunc('month', date_day) - Interval '1 month' FROM date_day))
),

sales_manager_data as 
(
        
        SELECT 
            regional,
            head_of_region 
        FROM dbt_prod.dim_sales_manager_data
),

sales_regional_data as 
(
    
    SELECT 
        DISTINCT
        kecamatan,
        city,
        province,
        sr.regional,
        user_id_kori,
        community_officer,
        manager,
        sm.head_of_region
    FROM dbt_prod.sales_regional_data sr
    LEFT JOIN sales_manager_data sm on lower(sm.regional) = lower(sr.regional) 
),

kori_area AS 
(
   
    SELECT 
        DISTINCT
        sc.month_at,
        sc.province_city_kecamatan
    FROM dbt_prod.seed_ce_area_project sc
    WHERE 
        sc.month_at IN ((SELECT DISTINCT date_trunc('month', date_day) FROM date_day), (SELECT DISTINCT date_trunc('month', date_day) - Interval '1 month' FROM date_day))
),

mart_incremental_monthly_region_reseller AS 
(
    
    SELECT 
        mi.*,
        {{snippet: new_region_2024Q3}}
        END AS new_region
    FROM dbt_prod.mart_incremental_monthly_region_reseller mi
    WHERE 
        month_at IN ((SELECT DISTINCT date_trunc('month', date_day) FROM date_day), (SELECT DISTINCT date_trunc('month', date_day) - Interval '1 month' FROM date_day))
),

mart_acq_phase AS 
(
    
    SELECT 
        user_id,
        month_at,
        acquisition_phase,
        LIFECYLE_GRADUATE_DATE,
        reseller_active_phase_level_1
    FROM dbt_prod.mart_reseller_acquisition_phase
    WHERE 
        month_at IN ((SELECT DISTINCT date_trunc('month', date_day) FROM date_day), (SELECT DISTINCT date_trunc('month', date_day) - Interval '1 month' FROM date_day))
        AND reseller_active_phase_level_1 != 'New Register' 
),

baseline_user AS
(
   
    SELECT 
        DISTINCT
        mr.user_id,
        mr.month_at,
        mir.province,
        mir.city,
        mir.kecamatan,
        sa.community_officer AS scs_name,
        sa.manager AS scm_name,
        mr.acquisition_phase,
        mr.LIFECYLE_GRADUATE_DATE,
        mir.new_region,
        
        CASE 
            WHEN mr.acquisition_phase IN ('Dormant Graduate','New Graduate M1','New Graduate Non M1','Returning Graduate') THEN 'Retention' 
            WHEN mr.acquisition_phase IN ('Registered Only 2024','Registered Only before 2024','Dormant Onboarding Digital','New Onboarding Digital','Returning Onboarding Digital') THEN 'Acquisition'
            WHEN mr.acquisition_phase IN ('Dormant Onboarding Physical','New Onboarding Physical','Returning Onboarding Physical') THEN 'Onboarding'
            ELSE 'Other'
        END AS team,
        
        CASE 
            WHEN ka.province_city_kecamatan IS NOT NULL THEN 1 
            ELSE 0 
        END AS is_in_area_kori,
        
        CASE 
            WHEN uj.province_city_kecamatan IS NOT NULL THEN 1 
            ELSE 0
        END AS is_in_kbb_project,
        
        CASE 
            WHEN mr.acquisition_phase IN ('Returning Onboarding Physical', 'New Onboarding Physical') AND is_in_kbb_project = 0 THEN 'RGC'
            WHEN mr.acquisition_phase IN ('Dormant Onboarding Physical') THEN 'Onboarding Dormant'
            WHEN mr.acquisition_phase IN ('Dormant Graduate','New Graduate M1','New Graduate Non M1','Returning Graduate') AND is_in_area_kori = 0 THEN 'AE'
            WHEN mr.acquisition_phase IN ('Dormant Graduate','New Graduate M1','New Graduate Non M1','Returning Graduate') AND is_in_area_kori = 1 THEN 'CE'
            WHEN mr.acquisition_phase IN ('Returning Onboarding Physical', 'New Onboarding Physical') AND is_in_kbb_project = 1 THEN 'CE'
            ELSE NULL 
        END AS handled_by 
        
    FROM mart_acq_phase mr 
    JOIN dbt_prod.gross_reseller gr 
        ON gr.user_id = mr.user_id
    LEFT JOIN mart_incremental_monthly_region_reseller mir
        ON mr.user_id = mir.user_id
        AND mir.month_at = mr.month_at
    LEFT JOIN sales_regional_data sa
        ON lower(sa.kecamatan)=lower(mir.kecamatan) 
        AND lower(sa.city)=lower(mir.city) 
        AND lower(sa.province)=lower(mir.province)
    LEFT JOIN kori_area ka 
        ON ka.province_city_kecamatan = mir.province_city_kecamatan
        AND ka.month_at = mr.month_at
    LEFT JOIN user_join_kbb_project uj 
        ON uj.province_city_kecamatan = mir.province_city_kecamatan
        AND uj.month_at = mr.month_at
    WHERE 
        team = 'Retention'
        -- [[AND CASE WHEN {{Performance_filter}} IN ('Regular Physical', 'Regular Blended') THEN gr.reseller_project = 'Regular' ELSE 1=1 END]]
    
     
),

leads_filter AS 
    (
    
      SELECT DISTINCT
        bu.user_id,
        bu.handled_by,
        bu.new_region,
        case 
            when bu.handled_by = 'RGC' AND bu.new_region = 'Regional 1' THEN 'Diana'
            when bu.handled_by = 'RGC' AND bu.new_region = 'Regional 2' THEN 'Efsa'
            when bu.handled_by = 'RGC' AND bu.new_region = 'Regional 3' THEN 'Morik'
            when bu.handled_by = 'Onboarding Dormant' then 'Feby'
            when bu.handled_by = 'AE' AND bu.new_region = 'Regional 1' THEN 'Sisma'
            when bu.handled_by = 'AE' AND bu.new_region = 'Regional 2' THEN 'Ririn'
            when bu.handled_by = 'AE' AND bu.new_region = 'Regional 3' THEN 'Martina'
            when bu.handled_by = 'CE' then scs_name
        else 'Non Mapping' end as handled_by_name,
        bu.team AS team_phase,
        sof.total_gmv,
        bu.LIFECYLE_GRADUATE_DATE,
        bu.acquisition_phase
    from baseline_user bu
    LEFT JOIN summary_order_filtered sof ON bu.user_id = sof.user_id
    where 1=1
    AND month_at IN (SELECT DISTINCT date_trunc('month', date_day) FROM date_day)
    GROUP BY ALL
),

agg_join_group AS (
            
            SELECT      
            lf.handled_by,
            lf.handled_by_name,
            lf.team_phase,
            lf.new_region,
            COUNT(CASE WHEN jg.current_status = 'Join' THEN lf.user_id END) total_join_group
     FROM leads_filter lf
     JOIN join_group jg
        ON jg.user_id = lf.user_id
    --  WHERE lf.handled_by_name != 'Non Mapping'
     group by ALL
),


agg_filter AS (
    
     SELECT 
            lf.handled_by,
            lf.handled_by_name,
            lf.team_phase,
            lf.new_region,
            COUNT(DISTINCT lf.user_id) as total_leads,
            ajg.total_join_group,
            (ajg.total_join_group/total_leads) AS join_group_rate,
            COUNT(DISTINCT CASE WHEN lf.total_gmv IS NOT NULL THEN lf.user_id END) as total_ar,
            (total_ar/total_leads) AS active_rate,
            -- COUNT(DISTINCT CASE WHEN LIFECYLE_GRADUATE_DATE IN (SELECT DISTINCT date_day FROM date_day) THEN user_id END) AS total_graduate,
            SUM(lf.total_gmv) AS total_gmv
     FROM leads_filter lf
     LEFT JOIN agg_join_group ajg ON lf.handled_by = ajg.handled_by AND lf.handled_by_name = ajg.handled_by_name AND lf.team_phase = ajg.team_phase AND lf.new_region = ajg.new_region
    --  WHERE lf.handled_by_name != 'Non Mapping'
     group by ALL
),

leads_previous AS 
    (
    
    SELECT DISTINCT
        bu.user_id,
        bu.handled_by,
        bu.new_region,
        case 
            when bu.handled_by = 'RGC' AND bu.new_region = 'Regional 1' THEN 'Jihan'
            when bu.handled_by = 'RGC' AND bu.new_region = 'Regional 2' THEN 'Efsa'
            when bu.handled_by = 'RGC' AND bu.new_region = 'Regional 3' THEN 'Morik'
            when bu.handled_by = 'Onboarding Dormant' then 'Feby'
            when bu.handled_by = 'AE' AND bu.new_region = 'Regional 1' THEN 'Sisma'
            when bu.handled_by = 'AE' AND bu.new_region = 'Regional 2' THEN 'Ririn'
            when bu.handled_by = 'AE' AND bu.new_region = 'Regional 3' THEN 'Martina'
            when bu.handled_by = 'CE' then scs_name
        else 'Non Mapping' end as handled_by_name,
        bu.team AS team_phase,
        sop.total_gmv,
        bu.LIFECYLE_GRADUATE_DATE
    from baseline_user bu
    LEFT JOIN summary_order_previous sop ON bu.user_id = sop.user_id
    where 1=1  
    AND month_at IN (SELECT DISTINCT date_trunc('month', DATEADD(month, -1, date_day)) FROM date_day)
    GROUP BY ALL
),

agg_previous AS (
    
     SELECT
            handled_by,
            handled_by_name,
            team_phase,
            new_region,
            COUNT(DISTINCT user_id) as total_leads,
            COUNT(DISTINCT CASE WHEN total_gmv IS NOT NULL THEN user_id END) as total_ar,
            (total_ar/total_leads) AS active_rate,
            -- COUNT(DISTINCT CASE WHEN LIFECYLE_GRADUATE_DATE IN (SELECT DISTINCT DATEADD(month, -1, date_day) FROM date_day) THEN user_id END) AS total_graduate,
            SUM(total_gmv) AS total_gmv
     FROM leads_previous
    --  WHERE handled_by_name != 'Non Mapping'
     group by ALL
),     

FINAL AS (
    
    SELECT 
          lf.handled_by,
          lf.handled_by_name,
          lf.team_phase,
          lf.new_region,
          lf.total_leads,
          lf.total_join_group,
          lf.join_group_rate,
          ta.target_ar,
          lf.total_ar,
          (lf.total_ar/ta.target_ar) ar_to_target,
          lf.active_rate,
          tg.target_gmv,
          lf.total_gmv,
          (lf.total_gmv/tg.target_gmv) AS gmv_to_target,
          lf.total_ar - COALESCE(lp.total_ar, 0) as mom_total_ar,
          lf.active_rate - COALESCE(lp.active_rate, 0) AS mom_active_rate,
          lf.total_gmv - COALESCE(lp.total_gmv, 0) AS mom_total_gmv,
        --   lf.total_graduate - COALESCE(lp.total_graduate, 0) AS mom_total_graduate
    FROM agg_filter lf
    LEFT JOIN agg_previous lp ON lf.handled_by = lp.handled_by AND lf.handled_by_name = lp.handled_by_name AND lf.new_region = lp.new_region
    LEFT JOIN target_gmv tg ON lf.handled_by_name = tg.pic_name
    LEFT JOIN target_ar ta ON lf.handled_by_name = ta.pic_name
)


SELECT * FROM final
order by 1,2,3
