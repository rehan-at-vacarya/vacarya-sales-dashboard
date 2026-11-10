-- ============================================================
-- Vacarya sales reporting model (BigQuery dataset Sales)
--   raw_events (append-only, written by n8n from GHL)
--   + history tables (frozen backfills): appt_contact_map, seed_first_booking,
--     stitch_followups, purchases, source_final (also fed daily by auto-tag)
--   -> stg_*  : cleaned events        -> dim_contact : 1 source per contact
--   -> fct_*  : calls, deals, wins, people (1 row per booked contact)
--   -> mart_* : daily aggregates       -> api_* : exactly what each dashboard endpoint serves
-- Both dashboard tabs read only api_* views, so every number comes from the same rows.
-- ============================================================

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.dim_contact` AS
-- One confirmed source per GHL contact: Webinar / VSL / E2 / Outbound / Referral (latest classification wins).
SELECT contact_id,
  ARRAY_AGG(source ORDER BY built_at DESC LIMIT 1)[OFFSET(0)] AS source,
  ARRAY_AGG(method ORDER BY built_at DESC LIMIT 1)[OFFSET(0)] AS source_method,
  ARRAY_AGG(conf   ORDER BY built_at DESC LIMIT 1)[OFFSET(0)] AS source_conf
FROM `nimble-album-506802-t2.Sales.source_final`
WHERE source IN ('Webinar','VSL','E2','Outbound','Referral') AND contact_id IS NOT NULL
GROUP BY contact_id;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.stg_appointments` AS
-- One row per GHL appointment on the 3 first-call calendars, latest known state.
-- Rows come from the GHL webhook, the hourly reconcile and the Sep backfill (no contact_id: filled from appt_contact_map).
WITH ev AS (
  SELECT
    r.received_at,
    JSON_VALUE(r.payload, '$.appointmentId') AS appointment_id,
    COALESCE(NULLIF(JSON_VALUE(r.payload, '$.contact_id'), ''), m.contact_id) AS contact_id,
    LOWER(TRIM(JSON_VALUE(r.payload, '$.contact_email'))) AS contact_email,
    NULLIF(TRIM(JSON_VALUE(r.payload, '$.contact_name')), '') AS contact_name,
    TRIM(JSON_VALUE(r.payload, '$.calendarName')) AS calendar,
    LOWER(IF(m.contact_id IS NOT NULL, COALESCE(m.ghl_status, JSON_VALUE(r.payload, '$.appoinmentStatus')),
             JSON_VALUE(r.payload, '$.appoinmentStatus'))) AS status,
    NULLIF(TRIM(IF(m.contact_id IS NOT NULL, COALESCE(m.ghl_closer, JSON_VALUE(r.payload, '$.closer')),
                   JSON_VALUE(r.payload, '$.closer'))), '') AS closer,
    LOWER(NULLIF(TRIM(JSON_VALUE(r.payload, '$.source')), '')) AS contact_type,
    COALESCE(SAFE_CAST(JSON_VALUE(r.payload, '$.date_created') AS TIMESTAMP), r.received_at) AS booked_at,
    SAFE_CAST(JSON_VALUE(r.payload, '$.startTime') AS DATETIME) AS start_local
  FROM `nimble-album-506802-t2.Sales.raw_events` r
  LEFT JOIN `nimble-album-506802-t2.Sales.appt_contact_map` m
    ON NULLIF(JSON_VALUE(r.payload, '$.contact_id'), '') IS NULL
   AND m.appointment_id = JSON_VALUE(r.payload, '$.appointmentId')
  WHERE r.event_type = 'appointment_status'
    AND TRIM(JSON_VALUE(r.payload, '$.calendarName')) IN
        ('Vacarya Strategy Call', 'Vacarya Partnership Call', 'Vacarya Strategy Call (Visapreneur Referral)')
    AND COALESCE(LOWER(JSON_VALUE(r.payload, '$.in_test')), '') NOT IN ('yes', 'true', '1')
)
SELECT * FROM (
SELECT
  appointment_id,
  ARRAY_AGG(contact_id    IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_id,
  ARRAY_AGG(contact_email IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_email,
  ARRAY_AGG(contact_name  IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_name,
  ARRAY_AGG(calendar      IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS calendar,
  ARRAY_AGG(closer        IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS closer,
  ARRAY_AGG(contact_type  IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_type,
  ARRAY_AGG(status ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS status,
  ARRAY_AGG(start_local IGNORE NULLS ORDER BY received_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS start_local,
  MIN(booked_at) AS booked_at
FROM ev
WHERE appointment_id IS NOT NULL
GROUP BY appointment_id
)
WHERE COALESCE(contact_id, '') NOT IN ('k5AZ2DLWTxqQqihamnu3','m37xXDPvV8soslks2E39')
  AND NOT REGEXP_CONTAINS(COALESCE(contact_email, ''), r'^(test\d*@|test@hotmail\.com$|rehan)');

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.stg_closer_logs` AS
-- Closer call logs from GHL (call_logged = the call outcome, deal_updated = the deal status), tests removed.
-- contact_id is filled from the email when GHL sent none; source comes from dim_contact.
WITH em AS (
  SELECT email, ARRAY_AGG(cid ORDER BY pri, ts DESC LIMIT 1)[OFFSET(0)] AS contact_id
  FROM (
    SELECT LOWER(TRIM(JSON_VALUE(payload, '$.contact_email'))) AS email, JSON_VALUE(payload, '$.contact_id') AS cid, 1 AS pri, received_at AS ts
    FROM `nimble-album-506802-t2.Sales.raw_events`
    WHERE NULLIF(JSON_VALUE(payload, '$.contact_id'), '') IS NOT NULL AND COALESCE(TRIM(JSON_VALUE(payload, '$.contact_email')), '') <> ''
    UNION ALL
    SELECT LOWER(TRIM(JSON_VALUE(r.payload, '$.contact_email'))), m.contact_id, 2, r.received_at
    FROM `nimble-album-506802-t2.Sales.raw_events` r
    JOIN `nimble-album-506802-t2.Sales.appt_contact_map` m ON m.appointment_id = JSON_VALUE(r.payload, '$.appointmentId')
    WHERE r.event_type = 'appointment_status' AND NULLIF(JSON_VALUE(r.payload, '$.contact_id'), '') IS NULL
      AND COALESCE(TRIM(JSON_VALUE(r.payload, '$.contact_email')), '') <> ''
  )
  GROUP BY email
),
e AS (
  SELECT
    received_at,
    DATE(received_at, 'America/Toronto')                         AS event_date,
    event_type,
    JSON_VALUE(payload, '$.opportunity_id')                      AS opportunity_id,
    NULLIF(JSON_VALUE(payload, '$.contact_id'), '')              AS contact_id_raw,
    LOWER(TRIM(JSON_VALUE(payload, '$.contact_email')))          AS contact_email,
    JSON_VALUE(payload, '$.contact_phone')                       AS contact_phone,
    JSON_VALUE(payload, '$.contact_name')                        AS contact_name,
    NULLIF(TRIM(JSON_VALUE(payload, '$.closer_outcome')), '')    AS call_outcome,
    NULLIF(TRIM(JSON_VALUE(payload, '$.deal_status')), '')       AS deal_status,
    NULLIF(TRIM(JSON_VALUE(payload, '$.objections')), '')        AS objections_raw,
    NULLIF(TRIM(JSON_VALUE(payload, '$.closer')), '')            AS closer,
    NULLIF(TRIM(JSON_VALUE(payload, '$.setter')), '')            AS setter,
    LOWER(NULLIF(TRIM(JSON_VALUE(payload, '$.source')), ''))     AS contact_type,
    COALESCE(SAFE_CAST(JSON_VALUE(payload, '$.prior_calls') AS INT64), 0) AS prior_calls
  FROM `nimble-album-506802-t2.Sales.raw_events`
  WHERE source_system = 'ghl'
    AND event_type IN ('call_logged', 'deal_updated')
    AND COALESCE(LOWER(JSON_VALUE(payload, '$.in_test')), '') NOT IN ('yes', 'true', '1')
)
SELECT
  e.received_at, e.event_date, e.event_type, e.opportunity_id,
  COALESCE(e.contact_id_raw, em.contact_id) AS contact_id,
  e.contact_email, e.contact_phone, e.contact_name, e.call_outcome, e.deal_status, e.objections_raw,
  e.closer, e.setter,
  COALESCE(dc.source, CASE e.contact_type WHEN 'webinar_lead' THEN 'Webinar' WHEN 'vsl_lead' THEN 'VSL'
                                          WHEN 'e2_lead' THEN 'E2' ELSE 'Outbound' END) AS source,
  e.prior_calls
FROM e
LEFT JOIN em ON e.contact_id_raw IS NULL AND em.email = e.contact_email
LEFT JOIN `nimble-album-506802-t2.Sales.dim_contact` dc ON dc.contact_id = COALESCE(e.contact_id_raw, em.contact_id)
WHERE COALESCE(COALESCE(e.contact_id_raw, em.contact_id), '') NOT IN ('k5AZ2DLWTxqQqihamnu3','m37xXDPvV8soslks2E39')
  AND NOT REGEXP_CONTAINS(COALESCE(e.contact_email, ''), r'^(test\d*@|test@hotmail\.com$|rehan)');

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.stg_setter_logs` AS
-- Latest setter outcome per contact (GHL Setter field + Benny's #sales-team backfill).
SELECT JSON_VALUE(payload, '$.contact_id') AS contact_id,
       JSON_VALUE(payload, '$.setter_outcome') AS setter_outcome,
       received_at
FROM `nimble-album-506802-t2.Sales.raw_events`
WHERE event_type = 'setter_logged' AND COALESCE(JSON_VALUE(payload, '$.setter_outcome'), '') <> ''
  AND JSON_VALUE(payload, '$.contact_id') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.contact_id') ORDER BY received_at DESC) = 1;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.fct_calls` AS
-- One row per call a closer logged. Re-logs of the same call within 2 hours count once (latest log wins).
-- call_date = the day the call HAPPENED, not the day it was logged: the contact's latest GHL appointment
-- (not cancelled) that started before the log and within 14 days.  If an earlier log already covers that
-- appointment, this log is a new call held off-calendar (e.g. a 2nd call on Google) -> the log day.
-- logged_date keeps the day the closer filled the form.
WITH src AS (
  SELECT * FROM `nimble-album-506802-t2.Sales.stg_closer_logs`
  WHERE event_type = 'call_logged' AND call_outcome IS NOT NULL
),
ordered AS (
  SELECT *, LAG(received_at) OVER (PARTITION BY opportunity_id ORDER BY received_at) AS prev_at FROM src
),
boundaries AS (
  SELECT *, CASE WHEN prev_at IS NULL THEN 1 WHEN TIMESTAMP_DIFF(received_at, prev_at, HOUR) >= 2 THEN 1 ELSE 0 END AS starts_new_call
  FROM ordered
),
grouped AS (
  SELECT *, SUM(starts_new_call) OVER (PARTITION BY opportunity_id ORDER BY received_at
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS call_group
  FROM boundaries
),
deduped AS (
  SELECT * FROM grouped
  QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, call_group ORDER BY received_at DESC, contact_email DESC) = 1
),
appts AS (
  SELECT contact_id, start_local FROM `nimble-album-506802-t2.Sales.stg_appointments`
  WHERE contact_id IS NOT NULL AND start_local IS NOT NULL AND COALESCE(status, '') != 'cancelled'
),
dated AS (
  SELECT d.*,
    (SELECT MAX(a.start_local) FROM appts a
      WHERE a.contact_id = d.contact_id
        AND a.start_local <= DATETIME(d.received_at, 'America/Toronto')
        AND a.start_local >= DATETIME_SUB(DATETIME(d.received_at, 'America/Toronto'), INTERVAL 14 DAY)) AS appt_start,
    LAG(d.received_at) OVER (PARTITION BY d.contact_id ORDER BY d.received_at) AS prev_log_at
  FROM deduped d
),
flagged AS (
  SELECT
    opportunity_id, contact_id, contact_email, contact_phone, contact_name,
    received_at,
    IF(appt_start IS NOT NULL AND (prev_log_at IS NULL OR DATETIME(prev_log_at, 'America/Toronto') < appt_start),
       DATE(appt_start), event_date) AS call_date,
    event_date AS logged_date,
    closer, setter, source, call_outcome, prior_calls,
    call_outcome = 'No Show'             AS is_no_show,
    STARTS_WITH(call_outcome, 'Live')    AS is_live,
    call_outcome = 'Live - No Offer'     AS is_no_offer,
    call_outcome = 'Live - Disqualified' AS is_dq,
    call_outcome = 'Live - Offer Made'   AS is_offer,
    call_outcome NOT IN ('No Show', 'Live - No Offer', 'Live - Disqualified', 'Live - Offer Made') AS is_unmapped
  FROM dated
)
SELECT *,
  ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY received_at) AS attempt_number,
  CASE WHEN is_live THEN
    prior_calls + SUM(CASE WHEN is_live THEN 1 ELSE 0 END) OVER (
      PARTITION BY opportunity_id ORDER BY received_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  END AS call_number
FROM flagged;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.fct_wins` AS
-- One row per paid deal.  Source of truth = GHL: closer fills the payment fields -> n8n "Deal Won"
-- -> raw_events deal_won (+ the Slack post).  Deals closed before that flow was used come from the
-- one-time history load in `purchases` (Sep 1 - Nov 8, checked against #payment-success).
WITH opp AS (
  SELECT opportunity_id, ANY_VALUE(contact_id) AS contact_id
  FROM `nimble-album-506802-t2.Sales.stg_closer_logs`
  WHERE opportunity_id IS NOT NULL AND contact_id IS NOT NULL
  GROUP BY 1
),
ghl AS (
  SELECT
    DATE(r.received_at, 'America/Toronto') AS closed_date,
    COALESCE(NULLIF(JSON_VALUE(r.payload, '$.contact_id'), ''), o.contact_id) AS contact_id,
    NULLIF(JSON_VALUE(r.payload, '$.opportunity_id'), '') AS opportunity_id,
    JSON_VALUE(r.payload, '$.contact_name') AS client,
    NULLIF(TRIM(JSON_VALUE(r.payload, '$.closer')), '') AS closer,
    NULLIF(TRIM(JSON_VALUE(r.payload, '$.setter')), '') AS setter,
    SAFE_CAST(JSON_VALUE(r.payload, '$.cash_collected') AS FLOAT64) AS collected,
    'GHL payment fields' AS proof,
    r.received_at
  FROM `nimble-album-506802-t2.Sales.raw_events` r
  LEFT JOIN opp o ON o.opportunity_id = JSON_VALUE(r.payload, '$.opportunity_id')
  WHERE r.event_type = 'deal_won'
    AND COALESCE(LOWER(JSON_VALUE(r.payload, '$.in_test')), '') NOT IN ('yes', 'true', '1')
    AND COALESCE(NULLIF(JSON_VALUE(r.payload, '$.contact_id'), ''), o.contact_id, '') NOT IN ('k5AZ2DLWTxqQqihamnu3','m37xXDPvV8soslks2E39')
    AND NOT REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(r.payload, '$.contact_email'), '')), r'^(test\d*@|test@hotmail\.com$|rehan)')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(JSON_VALUE(r.payload, '$.opportunity_id'), ''),
                                                    COALESCE(NULLIF(JSON_VALUE(r.payload, '$.contact_id'), ''), o.contact_id))
                             ORDER BY r.received_at) = 1
),
hist AS (
  SELECT closed_date, contact_id, CAST(NULL AS STRING) AS opportunity_id, client, closer,
         NULLIF(TRIM(setter), '') AS setter, CAST(collected_so_far AS FLOAT64) AS collected,
         'History load (Slack #payment-success, before GHL flow)' AS proof, loaded_at AS received_at
  FROM `nimble-album-506802-t2.Sales.purchases`
),
w AS (
  SELECT * FROM ghl
  UNION ALL
  SELECT * FROM hist
  WHERE hist.contact_id IS NULL
     OR hist.contact_id NOT IN (SELECT contact_id FROM ghl WHERE contact_id IS NOT NULL)
)
SELECT w.closed_date, w.contact_id, w.opportunity_id, w.client, w.closer, w.setter,
       COALESCE(dc.source, 'Outbound') AS source,
       25000 AS deal_value, w.collected, w.proof
FROM w
LEFT JOIN `nimble-album-506802-t2.Sales.dim_contact` dc USING (contact_id);

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.fct_deals` AS
-- Latest GHL deal status per opportunity (Follow-Up Booked / Decision Pending / Lost / Won) + objections.
WITH latest AS (
  SELECT * FROM `nimble-album-506802-t2.Sales.stg_closer_logs` WHERE deal_status IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY received_at DESC) = 1
),
objs AS (
  SELECT opportunity_id, objections_raw FROM `nimble-album-506802-t2.Sales.stg_closer_logs` WHERE objections_raw IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY received_at DESC) = 1
),
calls AS (
  SELECT opportunity_id, COUNTIF(is_live) AS live_calls, MAX(call_number) AS calls_to_date, MAX(call_date) AS last_call_date
  FROM `nimble-album-506802-t2.Sales.fct_calls` GROUP BY 1
)
SELECT
  l.opportunity_id, l.contact_id, l.contact_email, l.contact_name,
  l.closer, l.setter, l.source, l.deal_status,
  l.received_at AS status_at, l.event_date AS status_date,
  o.objections_raw,
  COALESCE(c.live_calls, 0) AS live_calls, c.calls_to_date, c.last_call_date,
  l.deal_status = 'Won'  AS is_won,
  l.deal_status = 'Lost' AS is_lost,
  l.deal_status IN ('Decision Pending', 'Follow-Up Booked') AS is_open,
  DATE_DIFF(CURRENT_DATE('America/Toronto'), l.event_date, DAY) AS days_since_update
FROM latest l
LEFT JOIN objs  o USING (opportunity_id)
LEFT JOIN calls c USING (opportunity_id);

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.fct_people` AS
-- ONE ROW PER PERSON who booked a first call (Strategy / Partnership / Visapreneur calendars), keyed by GHL contact_id.
-- Every "people who booked" number on both dashboard tabs is a count of this table.
--   booked_date = first booking (Toronto); closer/calendar = that first booking's
--   setter_bucket = setter outcome, else Canceled (booking cancelled, no call), else No Pickup
--   cancelled   = latest booking cancelled and no call logged
--   showed      = had at least one live call after booking;  noshow = never live, at least one No Show
--   call1_*     = outcome of the first logged call
--   call2_*     = 2nd call = anything after the first LIVE call (+2h).  Booked = closer set Follow-Up Booked,
--                 a new GHL appointment was booked, or a later call was logged.  Showed = any later live call;
--                 no-show = later No Show and no later live call.  Sep-Nov history (stitch_followups) counts
--                 too, and a live log always wins over the history (pending / claimed-only resolve themselves).
--                 For history people a bare "Follow-Up Booked" claim is not enough (audited: claimed_only = no 2nd call found).
--   won         = paid (fct_wins);  lost / open = latest GHL deal status when not paid
WITH a AS (SELECT * FROM `nimble-album-506802-t2.Sales.stg_appointments` WHERE contact_id IS NOT NULL),
first_bk AS (
  SELECT contact_id, MIN(booked_at) AS first_booked_at
  FROM (SELECT contact_id, booked_at FROM a
        UNION ALL SELECT contact_id, first_booked_at FROM `nimble-album-506802-t2.Sales.seed_first_booking`)
  WHERE contact_id IN (SELECT contact_id FROM a)
  GROUP BY 1
),
info AS (
  SELECT contact_id,
    ARRAY_AGG(closer        IGNORE NULLS ORDER BY booked_at LIMIT 1)[SAFE_OFFSET(0)] AS closer,
    ARRAY_AGG(calendar      IGNORE NULLS ORDER BY booked_at LIMIT 1)[SAFE_OFFSET(0)] AS calendar,
    ARRAY_AGG(contact_type  IGNORE NULLS ORDER BY booked_at LIMIT 1)[SAFE_OFFSET(0)] AS contact_type,
    ARRAY_AGG(contact_name  IGNORE NULLS ORDER BY booked_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_name,
    ARRAY_AGG(contact_email IGNORE NULLS ORDER BY booked_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS contact_email,
    ARRAY_AGG(STRUCT(status, start_local, closer) ORDER BY booked_at DESC LIMIT 1)[OFFSET(0)] AS last_appt,
    COUNT(*) AS appointments
  FROM a GROUP BY 1
),
calls AS (SELECT * FROM `nimble-album-506802-t2.Sales.fct_calls` WHERE contact_id IS NOT NULL),
pc AS (
  SELECT f.contact_id,
    ARRAY_AGG(c.call_outcome ORDER BY c.received_at LIMIT 1)[SAFE_OFFSET(0)] AS first_outcome,
    COUNT(*) AS n_logs,
    MIN(IF(c.is_live, c.received_at, NULL)) AS first_live_at,
    LOGICAL_OR(c.is_live)    AS ever_live,
    LOGICAL_OR(c.is_offer)   AS ever_offer,
    LOGICAL_OR(c.is_no_show) AS any_noshow,
    COUNTIF(c.is_live)       AS n_live
  FROM first_bk f JOIN calls c ON c.contact_id = f.contact_id AND c.received_at >= f.first_booked_at
  GROUP BY 1
),
fu_flag AS (
  SELECT p.contact_id, LOGICAL_OR(s.deal_status = 'Follow-Up Booked') AS fu_booked
  FROM pc p JOIN `nimble-album-506802-t2.Sales.stg_closer_logs` s
    ON s.contact_id = p.contact_id
   AND s.received_at BETWEEN TIMESTAMP_SUB(p.first_live_at, INTERVAL 10 MINUTE) AND TIMESTAMP_ADD(p.first_live_at, INTERVAL 2 HOUR)
  WHERE p.first_live_at IS NOT NULL
  GROUP BY 1
),
second_call AS (
  SELECT p.contact_id,
    ARRAY_AGG(c.call_outcome ORDER BY c.received_at LIMIT 1)[SAFE_OFFSET(0)] AS second_outcome,
    LOGICAL_OR(c.is_live)    AS live_after,
    LOGICAL_OR(c.is_no_show) AS noshow_after
  FROM pc p JOIN calls c ON c.contact_id = p.contact_id AND c.received_at > TIMESTAMP_ADD(p.first_live_at, INTERVAL 2 HOUR)
  WHERE p.first_live_at IS NOT NULL
  GROUP BY 1
),
rebook AS (
  SELECT DISTINCT p.contact_id
  FROM pc p JOIN a ON a.contact_id = p.contact_id AND a.booked_at > p.first_live_at
  WHERE p.first_live_at IS NOT NULL
),
deal AS (
  SELECT contact_id, ARRAY_AGG(deal_status ORDER BY status_at DESC LIMIT 1)[OFFSET(0)] AS deal_status
  FROM `nimble-album-506802-t2.Sales.fct_deals` WHERE contact_id IS NOT NULL GROUP BY 1
),
paid AS (
  SELECT contact_id, MIN(closed_date) AS won_date, SUM(collected) AS cash
  FROM `nimble-album-506802-t2.Sales.fct_wins` WHERE contact_id IS NOT NULL GROUP BY 1
),
base AS (
  SELECT
    f.contact_id, i.contact_name, i.contact_email,
    f.first_booked_at,
    DATE(f.first_booked_at, 'America/Toronto') AS booked_date,
    FORMAT_TIMESTAMP('%Y-%m', f.first_booked_at, 'America/Toronto') AS booked_month,
    COALESCE(i.closer, 'Unassigned') AS closer, i.calendar, i.contact_type,
    COALESCE(dc.source, CASE i.contact_type WHEN 'webinar_lead' THEN 'Webinar' WHEN 'vsl_lead' THEN 'VSL'
                                            WHEN 'e2_lead' THEN 'E2' ELSE 'Outbound' END) AS source,
    COALESCE(dc.source_method, 'CONTACT_TYPE') AS source_method,
    COALESCE(dc.source_conf, 'low') AS source_conf,
    sl.setter_outcome,
    i.appointments,
    i.last_appt.status AS last_status,
    i.last_appt.start_local AS last_start,
    i.last_appt.closer AS last_closer,
    p.contact_id IS NOT NULL AS has_call,
    p.first_outcome, COALESCE(p.n_logs, 0) AS n_logs,
    COALESCE(p.ever_live, FALSE)  AS showed,
    COALESCE(p.ever_offer, FALSE) AS offered,
    NOT COALESCE(p.ever_live, FALSE) AND COALESCE(p.any_noshow, FALSE) AS noshow,
    st.contact_id IS NOT NULL AS stitched,
    st.call2_status, fu.fu_booked, sc.second_outcome, p.n_live, st.live_calls AS st_live_calls,
    COALESCE(sc.live_after, FALSE) AS live_after, COALESCE(sc.noshow_after, FALSE) AS noshow_after,
    rb.contact_id IS NOT NULL AS rebooked,
    d.deal_status,
    pd.contact_id IS NOT NULL AS won, pd.won_date, COALESCE(pd.cash, 0) AS cash
  FROM first_bk f
  JOIN info i USING (contact_id)
  LEFT JOIN `nimble-album-506802-t2.Sales.dim_contact` dc USING (contact_id)
  LEFT JOIN `nimble-album-506802-t2.Sales.stg_setter_logs` sl USING (contact_id)
  LEFT JOIN pc p USING (contact_id)
  LEFT JOIN fu_flag fu USING (contact_id)
  LEFT JOIN second_call sc USING (contact_id)
  LEFT JOIN rebook rb USING (contact_id)
  LEFT JOIN `nimble-album-506802-t2.Sales.stitch_followups` st USING (contact_id)
  LEFT JOIN deal d USING (contact_id)
  LEFT JOIN paid pd USING (contact_id)
)
SELECT
  contact_id, contact_name, contact_email, first_booked_at, booked_date, booked_month,
  closer, calendar, source, source_method, source_conf, contact_type,
  appointments, last_status, last_start, last_closer,
  last_status = 'cancelled' AND NOT has_call AS cancelled,
  COALESCE(last_status, '') != 'cancelled' AND last_start < CURRENT_DATETIME('America/Toronto') AS due,
  COALESCE(last_status, '') != 'cancelled' AND last_start < CURRENT_DATETIME('America/Toronto') AND NOT has_call AS unlogged,
  has_call, n_logs,
  setter_outcome,
  CASE
    WHEN setter_outcome = 'Qualified' THEN 'Qualified'
    WHEN setter_outcome LIKE 'Not qualified%' THEN 'Unqualified'
    WHEN setter_outcome = 'Attempted' THEN 'No Pickup'
    WHEN last_status = 'cancelled' AND NOT has_call THEN 'Canceled'
    ELSE 'No Pickup'
  END AS setter_bucket,
  first_outcome,
  CASE WHEN first_outcome IS NULL THEN NULL WHEN first_outcome = 'No Show' THEN 'No Show' ELSE 'Showed' END AS first_call,
  COALESCE(first_outcome != 'No Show', FALSE) AS call1_showed,
  COALESCE(first_outcome = 'No Show', FALSE)  AS call1_noshow,
  showed, noshow, offered,
  COALESCE(call2_status IN ('showed', 'no_show', 'pending'), FALSE)
    OR (showed AND (rebooked OR second_outcome IS NOT NULL OR (NOT stitched AND COALESCE(fu_booked, FALSE)))) AS call2_booked,
  COALESCE(call2_status = 'showed', FALSE) OR live_after AS call2_showed,
  NOT (COALESCE(call2_status = 'showed', FALSE) OR live_after)
    AND (COALESCE(call2_status = 'no_show', FALSE) OR noshow_after) AS call2_noshow,
  COALESCE(call2_status = 'pending', FALSE) AND NOT live_after AND NOT noshow_after AS call2_pending,
  COALESCE(call2_status = 'claimed_only', FALSE) AND NOT live_after AND NOT noshow_after AS call2_claimed_only,
  CASE WHEN live_after THEN 'Live' WHEN noshow_after THEN 'No Show' ELSE COALESCE(call2_status, second_outcome) END AS second_outcome,
  GREATEST(COALESCE(st_live_calls, 0), COALESCE(n_live, 0)) AS live_calls,
  stitched,
  IF(won, 'Won', deal_status) AS deal_status,
  won, won_date, cash,
  NOT won AND deal_status = 'Lost' AS lost,
  NOT won AND deal_status IN ('Decision Pending', 'Follow-Up Booked') AS open
FROM base;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.mart_people_daily` AS
-- fct_people counted by first-booking day x closer x source.
SELECT
  booked_date, booked_month, closer, source,
  COUNT(*) AS booked,
  COUNTIF(setter_bucket = 'Qualified')   AS qualified,
  COUNTIF(setter_bucket = 'Unqualified') AS unqualified,
  COUNTIF(setter_bucket = 'No Pickup')   AS no_pickup,
  COUNTIF(setter_bucket = 'Canceled')    AS setter_canceled,
  COUNTIF(cancelled)  AS cancelled,
  COUNTIF(due)        AS due,
  COUNTIF(due AND has_call) AS due_logged,
  COUNTIF(unlogged)   AS unlogged,
  COUNTIF(showed)     AS showed,
  COUNTIF(noshow)     AS noshow,
  COUNTIF(offered)    AS offered,
  COUNTIF(call1_showed) AS call1_showed,
  COUNTIF(call1_noshow) AS call1_noshow,
  COUNTIF(call1_showed AND setter_bucket = 'Qualified') AS q_call1_showed,
  COUNTIF(call1_noshow AND setter_bucket = 'Qualified') AS q_call1_noshow,
  COUNTIF(call2_booked) AS call2_booked,
  COUNTIF(call2_showed) AS call2_showed,
  COUNTIF(call2_noshow) AS call2_noshow,
  COUNTIF(call2_pending) AS call2_pending,
  COUNTIF(call2_claimed_only) AS call2_claimed_only,
  SUM(live_calls) AS live_calls,
  COUNTIF(stitched) AS stitched,
  COUNTIF(won)  AS won,
  COUNTIF(lost) AS lost,
  COUNTIF(open) AS open
FROM `nimble-album-506802-t2.Sales.fct_people`
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.mart_activity_daily` AS
-- What happened each day: calls logged (call date), deal-status changes (status date), paid deals (closed date).
WITH paid AS (SELECT DISTINCT contact_id FROM `nimble-album-506802-t2.Sales.fct_wins` WHERE contact_id IS NOT NULL),
call_side AS (
  SELECT call_date AS occurred_date, closer, setter, source,
    COUNT(*) AS logged_calls, COUNTIF(is_no_show) AS no_shows, COUNTIF(is_live) AS live_calls,
    COUNTIF(is_no_offer) AS no_offers, COUNTIF(is_dq) AS closer_dqs, COUNTIF(is_offer) AS offers_made,
    COUNTIF(is_unmapped) AS unmapped_outcomes,
    COUNTIF(is_live AND call_number = 1) AS calls_1, COUNTIF(is_live AND call_number = 2) AS calls_2,
    COUNTIF(is_live AND call_number >= 3) AS calls_3,
    0 AS deals_won, 0 AS deals_lost, 0 AS follow_ups, 0 AS pending, 0 AS wins_1, 0 AS wins_2, 0 AS wins_3, 0.0 AS cash
  FROM `nimble-album-506802-t2.Sales.fct_calls` GROUP BY 1, 2, 3, 4
),
deal_side AS (
  SELECT d.status_date, d.closer, d.setter, d.source,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, COUNTIF(d.is_lost), COUNTIF(d.deal_status = 'Follow-Up Booked'), COUNTIF(d.deal_status = 'Decision Pending'),
    0, 0, 0, 0.0
  FROM `nimble-album-506802-t2.Sales.fct_deals` d LEFT JOIN paid p ON p.contact_id = d.contact_id
  WHERE p.contact_id IS NULL
  GROUP BY 1, 2, 3, 4
),
wk AS (
  SELECT w.closed_date, w.closer, w.setter, w.source, w.client, w.contact_id, w.collected,
    GREATEST(COALESCE(MAX(c.call_number), 1), 1) AS k
  FROM `nimble-album-506802-t2.Sales.fct_wins` w
  LEFT JOIN `nimble-album-506802-t2.Sales.fct_calls` c ON c.contact_id = w.contact_id AND c.call_date <= w.closed_date
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),
win_side AS (
  SELECT closed_date, closer, setter, source,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    COUNT(*), 0, 0, 0, COUNTIF(k = 1), COUNTIF(k = 2), COUNTIF(k >= 3), SUM(collected)
  FROM wk GROUP BY 1, 2, 3, 4
)
SELECT occurred_date, closer, setter, source,
  SUM(logged_calls) AS logged_calls, SUM(no_shows) AS no_shows, SUM(live_calls) AS live_calls,
  SUM(no_offers) AS no_offers, SUM(closer_dqs) AS closer_dqs, SUM(offers_made) AS offers_made,
  SUM(unmapped_outcomes) AS unmapped_outcomes,
  SUM(calls_1) AS calls_1, SUM(calls_2) AS calls_2, SUM(calls_3) AS calls_3,
  SUM(deals_won) AS deals_won, SUM(deals_lost) AS deals_lost,
  SUM(follow_ups) AS follow_ups_booked, SUM(pending) AS decision_pending,
  SUM(wins_1) AS wins_1, SUM(wins_2) AS wins_2, SUM(wins_3) AS wins_3,
  SUM(deals_won) * 25000 AS contract_value,
  SUM(cash) AS cash_collected
FROM (SELECT * FROM call_side UNION ALL SELECT * FROM deal_side UNION ALL SELECT * FROM win_side)
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.mart_objections` AS
-- Objections from the latest closer log per opportunity; is_won = that person paid.
SELECT d.status_date AS occurred_date, d.closer, d.setter, d.source,
  TRIM(objection) AS objection,
  w.contact_id IS NOT NULL AS is_won,
  COUNT(*) AS times_raised
FROM `nimble-album-506802-t2.Sales.fct_deals` d
CROSS JOIN UNNEST(SPLIT(d.objections_raw, ',')) AS objection
LEFT JOIN (SELECT DISTINCT contact_id FROM `nimble-album-506802-t2.Sales.fct_wins` WHERE contact_id IS NOT NULL) w ON w.contact_id = d.contact_id
WHERE d.objections_raw IS NOT NULL AND TRIM(objection) != ''
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.api_closer_performance` AS
-- Endpoint /webhook/bigquery-dashboard (Closer Performance tab).
-- Activity rows (calls, deals, wins by the day they happened) + people rows (ap_*, by first-booking day, from fct_people).
SELECT occurred_date, closer, setter, source,
  logged_calls, no_shows, live_calls, no_offers, closer_dqs, offers_made, unmapped_outcomes,
  calls_1, calls_2, calls_3, deals_won, deals_lost, follow_ups_booked, decision_pending,
  wins_1, wins_2, wins_3, contract_value, cash_collected,
  0 AS ap_booked, 0 AS ap_cancelled, 0 AS ap_showed, 0 AS ap_noshow, 0 AS ap_unlogged,
  0 AS ap_offered, 0 AS ap_won, 0 AS ap_lost, 0 AS ap_pending
FROM `nimble-album-506802-t2.Sales.mart_activity_daily`
UNION ALL
SELECT booked_date, closer, CAST(NULL AS STRING), source,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0,
  booked, cancelled, showed, noshow, unlogged, offered, won, lost, open
FROM `nimble-album-506802-t2.Sales.mart_people_daily`;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.api_appointments` AS
-- Endpoint /webhook/dashboard-appointments (Team table). Same people as fct_people.
SELECT booked_date AS occurred_date, closer, source, booked, cancelled, due, due_logged AS logged, unlogged
FROM `nimble-album-506802-t2.Sales.mart_people_daily`;

CREATE OR REPLACE VIEW `nimble-album-506802-t2.Sales.api_funnel_metrics` AS
-- Endpoint /webhook/dashboard-aiden (Funnel Metrics tab).
-- People rows by first-booking day (won = 0 here) + paid-deal rows by closed day (close rate counts deals closed in the period).
SELECT CAST(booked_date AS STRING) AS occurred_date, booked_month, closer, source,
  booked, qualified, unqualified, no_pickup, setter_canceled AS canceled,
  call1_showed, call1_noshow, q_call1_showed, q_call1_noshow,
  showed AS showed_any, offered, call2_booked, call2_showed, call2_noshow, call2_pending, call2_claimed_only,
  live_calls, stitched, 0 AS won, 0.0 AS cash
FROM `nimble-album-506802-t2.Sales.mart_people_daily`
UNION ALL
SELECT CAST(closed_date AS STRING), FORMAT_DATE('%Y-%m', closed_date), COALESCE(closer, 'Unassigned'), source,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  COUNT(*), SUM(collected)
FROM `nimble-album-506802-t2.Sales.fct_wins`
GROUP BY 1, 2, 3, 4;
