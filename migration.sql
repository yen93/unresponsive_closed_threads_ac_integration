-- Migration 1: unresponsive_lost_leads_columns_and_rpc
-- Guard columns + the single authoritative RPC the edge function calls.

ALTER TABLE public.follow_up_sequence_threads
  ADD COLUMN IF NOT EXISTS unresponsive_note_added_date timestamptz,
  ADD COLUMN IF NOT EXISTS unresponsive_tag_added_date  timestamptz;

ALTER TABLE public.cold_leads_follow_up_sequence_threads
  ADD COLUMN IF NOT EXISTS unresponsive_note_added_date timestamptz,
  ADD COLUMN IF NOT EXISTS unresponsive_tag_added_date  timestamptz;

-- Migration 3: unresponsive_lead_skip_guard
-- Skip guard column + RPC extended to expose the per-sequence final-email date
-- and exclude already-skipped leads.
ALTER TABLE public.follow_up_sequence_threads
  ADD COLUMN IF NOT EXISTS unresponsive_lead_processing_is_skipped boolean;
ALTER TABLE public.cold_leads_follow_up_sequence_threads
  ADD COLUMN IF NOT EXISTS unresponsive_lead_processing_is_skipped boolean;

-- Return type changed (adds final_email_date), so drop before recreating.
DROP FUNCTION IF EXISTS public.get_unresponsive_lost_leads();

CREATE FUNCTION public.get_unresponsive_lost_leads()
RETURNS TABLE (
  source_id                    bigint,
  source_table                 text,
  contact_id                   text,
  contact_email                text,
  deal_id                      text,
  lead_type                    text,
  final_email_date             timestamptz,
  unresponsive_note_added_date timestamptz,
  unresponsive_tag_added_date  timestamptz
)
LANGUAGE sql
STABLE
AS $$
  SELECT a.id::bigint, 'follow_up_sequence_threads'::text,
         b.id, b.email::text, c.id, 'soc med'::text,
         a.email_4th_date_sent,
         a.unresponsive_note_added_date, a.unresponsive_tag_added_date
  FROM public.follow_up_sequence_threads a
  JOIN public.activecampaign_contacts b ON a.email = b.email
  JOIN public.activecampaign_deals   c ON b.id   = c.contact
  WHERE a.email_4th_date_sent IS NOT NULL
    AND a.ac_lost_status_date IS NULL
    AND (a.unresponsive_lead_processing_is_skipped IS NOT TRUE)
  UNION ALL
  SELECT a.id::bigint, 'cold_leads_follow_up_sequence_threads'::text,
         b.id, b.email::text, c.id, 'cold'::text,
         a.email_5th_date_sent,
         a.unresponsive_note_added_date, a.unresponsive_tag_added_date
  FROM public.cold_leads_follow_up_sequence_threads a
  JOIN public.activecampaign_contacts b ON a.email = b.email
  JOIN public.activecampaign_deals   c ON b.id   = c.contact
  WHERE a.email_5th_date_sent IS NOT NULL
    AND a.ac_lost_status_date IS NULL
    AND (a.unresponsive_lead_processing_is_skipped IS NOT TRUE);
$$;

-- Migration 2: process_unresponsive_lost_leads_weekly_cron
-- Mon 6:30 AM PHT (UTC+8) => 22:30 UTC Sunday. Function is deployed with
-- verify_jwt = false, so no Authorization header / embedded token is needed.
select cron.schedule(
  'process-unresponsive-lost-leads-weekly',
  '30 22 * * 0',
  $$
  select net.http_post(
    url := 'https://aivitcomiywiysrfwqxt.supabase.co/functions/v1/process-unresponsive-lost-leads',
    headers := jsonb_build_object('Content-Type','application/json'),
    body := '{}'::jsonb,
    timeout_milliseconds := 30000);
  $$
);
