# BigQuery reporting model (dataset `nimble-album-506802-t2.Sales`)

`reporting_model.sql` is the full definition of every view. Run it top to bottom to rebuild.
Both dashboard tabs read only the `api_*` views, so every number comes from the same rows.

| Layer | Object | One row per | Written / built by |
|---|---|---|---|
| raw | `raw_events` | GHL event (appointment_status, call_logged, deal_updated, deal_won, setter_logged) | n8n: Dashboard webhook, hourly appt reconcile, Deal Won flow |
| history | `appt_contact_map`, `seed_first_booking`, `stitch_followups`, `purchases` | frozen Sep 1 – Nov 8 backfills | one-time loads (do not edit) |
| history + daily | `source_final` | contact source classification | one-time load + n8n daily auto-tag |
| staging | `stg_appointments`, `stg_closer_logs`, `stg_setter_logs` | cleaned event | view |
| dimension | `dim_contact` | contact → Webinar / VSL / E2 / Outbound / Referral | view |
| facts | `fct_people` | person who booked a first call (contact_id) | view |
| | `fct_calls` | call a closer logged | view |
| | `fct_deals` | opportunity (latest deal status) | view |
| | `fct_wins` | paid deal (GHL payment fields; history from `purchases`) | view |
| marts | `mart_people_daily`, `mart_activity_daily`, `mart_objections` | day × closer × source | view |
| api | `api_closer_performance`, `api_appointments`, `api_funnel_metrics` (+ `mart_objections`) | exactly what each n8n endpoint returns | view |

Rules: GHL is the only source of truth. Slack is never read. Tests (`k5AZ2DLWTxqQqihamnu3`, `m37xXDPvV8soslks2E39`, test@/rehan emails, `in_test`) are removed in staging.
