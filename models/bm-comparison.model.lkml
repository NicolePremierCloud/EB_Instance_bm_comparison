connection: "ga-360-eb"

include: "/views/*.view.lkml"                # include all views in the views/ folder in this project



explore: audience_overview{
  label: "audience overview_comparison"
  always_filter: {
    filters: [first_period_filter: "NOT NULL", second_period_filter: "NOT NULL", period_selected:"-NULL"]
  }}
