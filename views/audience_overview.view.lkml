
view: audience_overview {
  derived_table: {
    sql:
    SELECT
      PARSE_DATE("%Y%m%d", date) as date,
      PARSE_DATE("%Y%m%d", SUBSTR(_TABLE_SUFFIX, -8)) as TABLE_SUFFIX,
      3016209496 as stream_id,
      hits.page.hostname as hostname,
      device.deviceCategory as category,
      geoNetwork.country,
      CONCAT(trafficsource.source, ' / ', trafficsource.medium) as source_medium,
      FullvisitorID as UserID,
      visitNumber,
      totals.visits,
      totals.pageviews,
      totals.timeonsite,
      totals.bounces,
      hits.page.pagepath as pagepath,
      hits.page.pagetitle as pagetitle,
      hits.eventinfo.EventCategory as event_category,
      hits.eventinfo.EventAction as event_action,
      hits.eventinfo.eventlabel as event_label,
      Concat(visitid,visitstarttime,fullvisitorid) as sessionID,
      (SELECT
            MAX(
              CASE
                WHEN hits.isEntrance AND hits.type = 'PAGE'
                  THEN hits.page.pagePath
              END
            ) as lp
          FROM UNNEST(ga_sessions.hits) as hits)
      AS Landing_page,
      CASE
          WHEN visitNumber = 1 THEN 'New User'
          ELSE 'Returning User'
      END AS user_type,
      device.language  AS language,
      trafficSource.campaign,
      trafficSource.referralPath as referral_path,
      CASE
        WHEN {% condition first_period_filter %} CAST(PARSE_DATE("%Y%m%d", date) AS TIMESTAMP) {% endcondition %}
        THEN 'First Period'
        WHEN {% condition second_period_filter %} CAST(PARSE_DATE("%Y%m%d", date) AS TIMESTAMP) {% endcondition %}
        THEN 'Second Period'
      END AS audience_overview_period_selected,
      ROW_NUMBER() OVER(PARTITION BY Concat(visitid,visitstarttime,fullvisitorid) ORDER BY Concat(visitid,visitstarttime,fullvisitorid)) as rn,
    FROM `eb-seo.102352566.ga_sessions_*` as ga_sessions,
    UNNEST(hits) as hits
    Where(      CASE
        WHEN {% condition first_period_filter %} CAST(PARSE_DATE("%Y%m%d", SUBSTR(_TABLE_SUFFIX, -8)) AS TIMESTAMP) {% endcondition %}
        THEN 'First Period'
        WHEN {% condition second_period_filter %} CAST(PARSE_DATE("%Y%m%d", SUBSTR(_TABLE_SUFFIX, -8)) AS TIMESTAMP) {% endcondition %}
        THEN 'Second Period'
    END) is not null


           ;;
  }

  dimension_group: created {
    type: time
    datatype: date
    view_label: "Date_Axis"
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week,
      day_of_week_index,
      day_of_month,
      day_of_year,
      week,
      week_of_year,
      month,
      month_name,
      month_num,
      quarter,
      year
    ]
    sql: ${TABLE}.date ;;
    convert_tz: no
  }



  ## ------------------ USER FILTERS  ------------------ ##

  filter: first_period_filter {
    view_label: "_PoP"
    group_label: "Arbitrary Period Comparisons"
    description: "Choose the first date range to compare against. This must be before the second period"
    type: date
  }

  filter: second_period_filter {
    view_label: "_PoP"
    group_label: "Arbitrary Period Comparisons"
    description: "Choose the second date range to compare to. This must be after the first period"
    type: date
  }

  ## ------------------ START HIDDEN HELPER DIMENSIONS  ------------------ ##

  dimension: days_from_start_first {
   view_label: "_PoP"
    hidden: yes
    type: number
    sql: date_diff(CAST(${created_date} AS TIMESTAMP),{% date_start first_period_filter %} ,DAY) ;;
  }

  dimension: days_from_start_second {
    view_label: "_PoP"
    hidden: yes
    type: number
    sql: date_diff( CAST(${created_date} AS TIMESTAMP),{% date_start second_period_filter %},DAY) ;;
  }

  ## ------------------ END HIDDEN HELPER DIMENSIONS  ------------------ ##


  ## ------------------ DIMENSIONS TO PLOT ------------------ ##

  dimension: days_from_first_period {
    view_label: "_PoP"
    description: "Select for Grouping (Rows)"
    group_label: "Arbitrary Period Comparisons"
    type: number
    hidden: no
    sql:
            CASE
            WHEN ${days_from_start_second} >= 0
           THEN ${days_from_start_second}
           WHEN ${days_from_start_first} >= 0
            THEN ${days_from_start_first}
            END;;
  }


  dimension: period_selected {
    view_label: "_PoP"
    group_label: "Arbitrary Period Pivot"
    label: "First or second period"
    description: "Select for Comparison (Pivot)"
    type: string
    sql: ${TABLE}.audience_overview_period_selected;;
  }



  ## ------------------ END DIMENSIONS TO PLOT ------------------ ##



# Dimensions not used in measures

  dimension: date {
    type: date
    sql: ${TABLE}.date;;
  }

  dimension: stream_id {
    type: number
    sql: ${TABLE}.stream_id;;
  }

  dimension: rn {
    type: number
    sql: ${TABLE}.rn;;
    hidden: yes
  }

  dimension: UserID {
    type: string
    sql: ${TABLE}.UserID;;
    hidden: yes
  }

  dimension: sessionID {
    type: string
    sql: ${TABLE}.sessionID;;
    hidden: yes
  }

  dimension: hostname {
    type: string
    sql: ${TABLE}.hostname;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country;;
  }

  dimension: source_medium {
    type: string
    sql: ${TABLE}.source_medium;;
  }


  dimension: Page_Title {
    type: string
    sql: ${TABLE}.pagetitle;;
  }
  dimension: Referral_Path {
    type: string
    sql: ${TABLE}.referral_path;;
  }
  dimension: Landing_Page {
    type: string
    sql: ${TABLE}.Landing_page;;
  }

  dimension: event_category {
    type: string
    sql: ${TABLE}.event_category;;
  }

  dimension: event_action {
    type: string
    sql: ${TABLE}.event_action;;
  }

  dimension: event_label {
    type: string
    sql: ${TABLE}.eventlabel;;
  }
  dimension: Page_Path {
    type: string
    sql: ${TABLE}.pagepath;;
    drill_fields: [Page_Title,event_category,event_action,event_label]
  }

  dimension: user_type {
    type: string
    sql: ${TABLE}.user_type;;
  }

  dimension: language {
    type: string
    sql: ${TABLE}.language;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign;;
  }

  dimension: visitNumber {
    type: number
    sql: ${TABLE}.visitNumber;;
    hidden: yes
  }

  dimension: visits {
    type: number
    sql: ${TABLE}.visits;;
    hidden: yes
  }

  dimension: pageviews {
    type: number
    sql: ${TABLE}.pageviews;;
    hidden:  yes
  }

  dimension: timeonsite {
    type: number
    sql: ${TABLE}.timeonsite;;
    hidden:  yes
  }

  dimension: bounces {
    type: number
    sql: ${TABLE}.bounces;;
    hidden: yes
  }

# Measures

   measure: current_period_total_users {
     view_label: "Measures"
     group_label: "Total Users"
     type: count_distinct
     sql: ${UserID};;
     filters: [period_selected: "Second Period"]
   }

   measure: previous_period_total_users {
     view_label: "Measures"
     group_label: "Total Users"
    type: count_distinct
     sql: ${UserID};;
     filters: [period_selected: "First Period"]
   }

   measure: total_users_pop_change {
     view_label: "Measures"
     group_label: "Total Users"
     label: "Total Users period-over-period % change"
     type: number
     sql: (1.0 * ${current_period_total_users} / IFNULL(${previous_period_total_users} ,0)) - 1 ;;
     value_format: "0.00%"
   }

  measure: total_users_overall {
    view_label: "Measures"
    group_label: "Total Users"
    type: count_distinct
    sql: ${UserID};;
  }

   measure: current_period_pct_total_users {
     view_label: "Measures"
     group_label: "% Total Users"
     type: count_distinct
     sql: CASE WHEN ${visitNumber} = 1 THEN ${UserID} ELSE NULL END;;
     filters: [period_selected: "Second Period"]
   }

   measure: previous_period_pct_total_users {
     view_label: "Measures"
     group_label: "% Total Users"
     type: count_distinct
     sql: CASE WHEN ${visitNumber} = 1 THEN ${UserID} ELSE NULL END;;
     filters: [period_selected: "First Period"]
   }

   measure: pct_total_users_pop_change {
     view_label: "Measures"
     group_label: "% Total Users"
     label: "% Total Users period-over-period % change"
     type: number
     sql: (1.0 * ${current_period_pct_total_users} / IFNULL(${previous_period_pct_total_users} ,0)) - 1 ;;
     value_format: "0.00%"
   }

   measure: pct_total_users_overall {
     view_label: "Measures"
     group_label: "% Total Users"
     type: number
     sql: COUNT(DISTINCT CASE WHEN ${visitNumber} = 1 THEN ${UserID} ELSE NULL END);;
   }

   measure: current_period_new_users {
     view_label: "Measures"
     group_label: "New Users"
     type: count_distinct
     sql: CASE WHEN ${visitNumber} = 1 THEN ${UserID} ELSE NULL END;;
     filters: [period_selected: "Second Period"]
   }

   measure: previous_period_new_users {
     view_label: "Measures"
     group_label: "New Users"
     type: count_distinct
     sql: CASE WHEN ${visitNumber} = 1 THEN ${UserID} ELSE NULL END;;
     filters: [period_selected: "First Period"]
   }

  measure: new_users_pop_change {
    view_label: "Measures"
    group_label: "New Users"
    label: "New Users period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_new_users} / IFNULL(${previous_period_new_users} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: new_users_overall {
    view_label: "Measures"
    group_label: "New Users"
    type: count_distinct
    sql: CASE WHEN ${visitNumber} = 1 THEN ${UserID} ELSE NULL END;;
  }

  measure: current_period_sessions {
    view_label: "Measures"
    group_label: "Sessions"
    type: count_distinct
    sql: ${sessionID};;
    filters: [period_selected: "Second Period"]
  }

  measure: previous_period_sessions {
    view_label: "Measures"
    group_label: "Sessions"
    type: count_distinct
    sql: ${sessionID};;
    filters: [period_selected: "First Period"]
  }

  measure: sessions_pop_change {
    view_label: "Measures"
    group_label: "Sessions"
    label: "Sessions period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_sessions} / IFNULL(${previous_period_sessions} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: sessions_overall {
    view_label: "Measures"
    group_label: "Sessions"
    type: count_distinct
    sql: ${sessionID};;
  }


  measure: current_period_total_events {
    view_label: "Measures"
    group_label: "Total Events"
    type: count_distinct
    filters: [period_selected: "Second Period"]
  }

  measure: previous_period_total_events {
    view_label: "Measures"
    group_label: "Total Events"
    type: count_distinct
    filters: [period_selected: "First Period"]
  }

  measure: Total_events_pop_change {
    view_label: "Measures"
    group_label: "Total Events"
    label: "Total Events period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_total_events} / IFNULL(${previous_period_total_events} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: total_events_overall {
    view_label: "Measures"
    group_label: "Total Events"
    type: count_distinct
  }

  measure: current_period_sessions_per_user {
    view_label: "Measures"
    group_label: "Sessions Per User"
    type: number
    sql: (1.0 * ${current_period_sessions} / ${current_period_total_users}) ;;
    value_format: "0.00"

  }

  measure: previous_period_sessions_per_user {
    view_label: "Measures"
    group_label: "Sessions Per User"
    type: number
    sql: (1.0 *${previous_period_sessions} / ${previous_period_total_users}) ;;
    value_format: "0.00"
  }

  measure: sessions_per_user_pop_change {
    view_label: "Measures"
    group_label: "Sessions Per User"
    label: "Sessions Per User period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_sessions_per_user} / IFNULL(${previous_period_sessions_per_user} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: sessions_per_user_overall {
    view_label: "Measures"
    group_label: "Sessions Per User"
    type: number
    sql: ${sessions_overall} / ${total_users_overall} ;;
  }


  measure: current_period_events_per_user {
    view_label: "Measures"
    group_label: "Events Per User"
    type: number
    sql: (1.0 * ${current_period_total_events} / ${current_period_total_users}) ;;
    value_format: "0.00"

  }

  measure: previous_period_events_per_user {
    view_label: "Measures"
    group_label: "Events Per User"
    type: number
    sql: (1.0 *${previous_period_total_events} / ${previous_period_total_users}) ;;
    value_format: "0.00"
  }

  measure: events_per_user_pop_change {
    view_label: "Measures"
    group_label: "Sessions Per User"
    label: "Sessions Per User period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_events_per_user} / IFNULL(${previous_period_events_per_user} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: events_per_user_overall {
    view_label: "Measures"
    group_label: "Sessions Per User"
    type: number
    sql: ${total_events_overall} / ${total_users_overall} ;;
  }

  measure: current_period_pageviews {
    view_label: "Measures"
    group_label: "Pageviews"
    type: sum
    sql: CASE WHEN ${rn} = 1 THEN ${pageviews} ELSE 0 END;;
    filters: [period_selected: "Second Period"]
  }

  measure: previous_period_pageviews {
    view_label: "Measures"
    group_label: "Pageviews"
    type: sum
    sql: CASE WHEN ${rn} = 1 THEN ${pageviews} ELSE 0 END;;
    filters: [period_selected: "First Period"]
  }

  measure: pageviews_pop_change {
    view_label: "Measures"
    group_label: "Pageviews"
    label: "Pageviews period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_pageviews} / IFNULL(${previous_period_pageviews} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: pageviews_overall {
    view_label: "Measures"
    group_label: "Pageviews"
    type: sum
    sql: CASE WHEN ${rn} = 1 THEN ${pageviews} ELSE 0 END;;
  }


  measure: current_period_pageviews_per_user {
    view_label: "Measures"
    group_label: "Pageviews Per User"
    type: sum
    sql: 1.0 * (${current_period_pageviews}/${current_period_total_users});;
    value_format: "0.00"
  }

  measure: previous_period_pageviews_per_user {
    view_label: "Measures"
    group_label: "Pageviews Per User"
    type: sum
    sql: 1.0 * (${previous_period_pageviews}/${previous_period_pageviews});;
    value_format: "0.00"
  }

  measure: pageviews_per_user_pop_change {
    view_label: "Measures"
    group_label: "Pageviews Per User"
    label: "Pageviews period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_pageviews_per_user} / IFNULL(${previous_period_pageviews_per_user} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: pageviews_per_user_overall {
    view_label: "Measures"
    group_label: "Pageviews Per User"
    type: number
    sql: ${pageviews_overall}/${total_users_overall};;
    value_format: "0.00"
  }

  measure: current_period_pages_per_session {
    view_label: "Measures"
    group_label: "Pages Per Session"
    type: number
    sql: ${current_period_pageviews} / ${current_period_sessions} ;;
    value_format: "0.00"
  }

  measure: previous_period_pages_per_session {
    view_label: "Measures"
    group_label: "Pages Per Session"
    type: number
    sql: ${previous_period_pageviews} / ${previous_period_sessions} ;;
    value_format: "0.00"

  }

  measure: pages_per_session_pop_change {
    view_label: "Measures"
    group_label: "Pages Per Session"
    label: "Pages Per Session period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_pages_per_session} / IFNULL(${previous_period_pages_per_session} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: pages_per_session_overall {
    view_label: "Measures"
    group_label: "Pages Per Session"
    type: number
    sql: ${pageviews_overall} / ${sessions_overall} ;;
    value_format: "0.00"
  }

  measure: current_period_bounces {
    hidden: yes
    type:  sum
    sql:  CASE WHEN ${rn} = 1 THEN ${bounces} ELSE 0 END;;
    filters: [period_selected: "Second Period"]
  }


  measure: previous_period_bounces {
    hidden: yes
    type:  sum
    sql:  CASE WHEN ${rn} = 1 THEN ${bounces} ELSE 0 END;;
    filters: [period_selected: "First Period"]
  }


  measure: current_period_visits {
    hidden: yes
    type:  sum
    sql:  CASE WHEN ${rn} = 1 THEN ${visits} ELSE 0 END;;
    filters: [period_selected: "Second Period"]
  }

  measure: previous_period_timeonsite {
    hidden: yes
    type:  sum
    sql:  CASE WHEN ${rn} = 1 THEN ${timeonsite} ELSE 0 END;;
    filters: [period_selected: "First Period"]
  }


  measure: current_period_timeonsite {
    hidden: yes
    type:  sum
    sql:  CASE WHEN ${rn} = 1 THEN ${timeonsite} ELSE 0 END;;
    filters: [period_selected: "Second Period"]
  }


  measure: previous_period_visits {
    hidden: yes
    type:  sum
    sql:  CASE WHEN ${rn} = 1 THEN ${visits} ELSE 0 END;;
    filters: [period_selected: "First Period"]
  }


  measure: current_period_bounce_rate {
    view_label: "Measures"
    group_label: "Bounce Rate"
    type: number
    sql: 1.0 * ${current_period_bounces} / ${current_period_visits} ;;
    value_format: "0.00%"
  }

  measure: previous_period_bounce_rate {
    view_label: "Measures"
    group_label: "Bounce Rate"
    type: number
    sql: 1.0 * ${previous_period_bounces} / ${previous_period_visits} ;;
    value_format: "0.00%"
  }

  measure: bounce_rate_pop_change {
    view_label: "Measures"
    group_label: "Bounce Rate"
    label: "Bounce Rate period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_bounce_rate} / IFNULL(${previous_period_bounce_rate} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: bounce_rate_overall {
    view_label: "Measures"
    group_label: "Bounce Rate"
    type: number
    sql: 1.0 * IFNULL(SUM(CASE WHEN ${rn} = 1 THEN ${bounces} ELSE 0 END), 0) / IFNULL(SUM(CASE WHEN ${rn} = 1 THEN ${visits} ELSE 0 END), 0) ;;
    value_format: "0.00%"
  }

  measure: current_period_eng_time_per_session {
    view_label: "Measures"
    group_label: "Eng. Time Per Session"
    type: number
    sql: (IFNULL(${current_period_timeonsite}, 0) / ${current_period_visits})/ 86400 ;;
    value_format: "hh:mm:ss"
  }

  measure: previous_period_eng_time_per_session {
    view_label: "Measures"
    group_label: "Eng. Time Per Session"
    type: number
    sql: (IFNULL( ${previous_period_timeonsite}, 0) / IFNULL(${previous_period_visits}, 0))/ 86400 ;;
    value_format: "hh:mm:ss"

  }

  measure: eng_time_per_session_pop_change {
    view_label: "Measures"
    group_label: "Eng. Time Per Session"
    label: "Eng. Time Per Session period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_eng_time_per_session} / IFNULL(${previous_period_eng_time_per_session} ,0)) - 1 ;;
    value_format: "0.00%"
  }

  measure: eng_time_per_session_overall {
    view_label: "Measures"
    group_label: "Eng. Time Per Session"
    type: number
    sql: FLOOR(IFNULL(SUM(CASE WHEN ${rn} = 1 THEN ${timeonsite} ELSE 0 END), 0) / IFNULL(SUM(CASE WHEN ${rn} = 1 THEN ${visits} ELSE 0 END), 0)) ;;
    value_format: "hh:mm:ss"
  }


  measure: current_period_Avg_Engagemet_Time {
    view_label: "Measures"
    group_label: "Avg. Engagement Time"
    type: number
    sql: (IFNULL(${current_period_timeonsite}, 0) / ${current_period_sessions})/ 86400 ;;
    value_format: "hh:mm:ss"
  }

  measure: previous_period_Avg_Engagemet_Time {
    view_label: "Measures"
    group_label: "Avg. Engagement Time"
    type: number
    sql: (IFNULL( ${previous_period_timeonsite}, 0) / IFNULL(${previous_period_sessions}, 0))/ 86400 ;;
    value_format: "hh:mm:ss"

  }

  measure: Avg_Engagemet_Time_pop_change {
    view_label: "Measures"
    group_label: "Avg. Engagement Time"
    label: "Avg. Engagement Time period-over-period % change"
    type: number
    sql: (1.0 * ${current_period_Avg_Engagemet_Time} / IFNULL(${previous_period_Avg_Engagemet_Time} ,0)) - 1 ;;
    value_format: "0.00%"
  }


  measure: Avg_Engagemet_Time_overall {
    view_label: "Measures"
    group_label: "Avg. Engagement Time"
    type: number
    sql: FLOOR(IFNULL(SUM(CASE WHEN ${rn} = 1 THEN ${timeonsite} ELSE 0 END), 0) / ${sessions_overall}, 0)/86400) ;;
    value_format: "hh:mm:ss"
  }





}
