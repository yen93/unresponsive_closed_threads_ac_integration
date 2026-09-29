// Supabase Edge Function: process-unresponsive-lost-leads
//
// Weekly (Mon 6:30 AM PHT) automation. Finds every lead that received the FINAL
// email of its follow-up sequence, never replied, and has not been closed out
// yet (ac_lost_status_date IS NULL) -- across BOTH sequences:
//   - soc med -> follow_up_sequence_threads          (4-email sequence)
//   - cold    -> cold_leads_follow_up_sequence_threads (5-email sequence)
// The single authoritative query lives in the SQL function
// get_unresponsive_lost_leads() and is called via supabase.rpc().
//
// SKIP GUARD (runs first, before any AC change): a lead that is still being
// worked by the sales team must NOT be auto-closed. If the deal has a note in AC
// authored by a user OTHER than Julienne (the API-token owner, whose notes are
// automations) dated AFTER the sequence's final email, we set
// unresponsive_lead_processing_is_skipped = true and make no AC changes.
//
// Otherwise, three ActiveCampaign (AC) actions run in order, each guarded by its
// own timestamp column so a partial failure resumes cleanly next run with no
// duplicate note and no duplicate tag:
//   1. Add a note to the deal      -> stamp unresponsive_note_added_date
//   2. Tag the contact unresponsive_contact -> stamp unresponsive_tag_added_date
//   3. Set the deal status to Lost -> stamp ac_lost_status_date (done marker)
// The Supabase timestamp is written ONLY after the AC call succeeds, so a failed
// AC call leaves the row for the next run to retry (idempotent).
//
// Triggered by pg_cron (net.http_post). Kicks the real work into a background
// task and returns 202 immediately so it never hits the request timeout.
// Debug params: ?sync=1 (run inline, return full summary), ?limit=N.

import { createClient, SupabaseClient } from "jsr:@supabase/supabase-js@2";

// Supabase edge runtime global for background work.
declare const EdgeRuntime: { waitUntil(p: Promise<unknown>): void } | undefined;

// AC deal status enum: 0 = Open, 1 = Won, 2 = Lost
const AC_STATUS_LOST = 2;
const TAG_NAME = "unresponsive_contact";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const AC_API_URL = (Deno.env.get("AC_API_URL") ?? "").replace(/\/+$/, "");
const AC_API_TOKEN = Deno.env.get("AC_API_TOKEN") ?? "";

// The API-token owner (Julienne Mae Anito) authors this function's own notes and
// most other automations, so her notes never count as human engagement. Proven
// empirically: notes this function creates show userid 27. Override via env if
// the API token ever changes owner.
const JULIENNE_AC_USER_ID = Deno.env.get("JULIENNE_AC_USER_ID") ?? "27";

interface Lead {
  source_id: number;
  source_table: string;
  contact_id: string | null;
  contact_email: string;
  deal_id: string | null;
  lead_type: string;
  final_email_date: string | null;
  unresponsive_note_added_date: string | null;
  unresponsive_tag_added_date: string | null;
}

interface RunResult {
  ran_at: string;
  processed: number;
  succeeded: number; // leads that reached "deal lost" this run
  skipped: number;   // leads held back (still being worked by a human)
  failed: number;    // leads that threw on some step this run
  errors: Array<{ email: string; deal: string | null; step: string; error: string }>;
}

const acHeaders = {
  "Api-Token": AC_API_TOKEN,
  "Content-Type": "application/json",
};

interface DealNote {
  userid: string;
  cdate: string;
}

// Returns true if the deal has a note authored by someone other than Julienne
// (and not the "0"/system user) dated strictly after finalEmailDate -- meaning
// the lead is still being worked and must be skipped. Returns false when there
// is no such note or finalEmailDate is unknown. Throws on AC/network error so
// the caller can leave the row for the next run rather than risk a wrong close.
//
// Uses /deals/{id}/notes (note-only, each carries userid + cdate) rather than
// /dealActivities (which mixes in tasks/status/field changes, ignores a
// dataType filter, and needs careful paging). Notes per deal are few, but we
// still page defensively, advancing offset by the ACTUAL returned count (AC may
// return fewer rows than the requested limit).
async function hasHumanNoteAfterFinalEmail(
  dealId: string,
  finalEmailDate: string | null,
): Promise<boolean> {
  if (!finalEmailDate) return false;
  const finalMs = Date.parse(finalEmailDate);
  if (Number.isNaN(finalMs)) return false;

  let offset = 0;
  const pageSize = 100;
  let latestHumanNoteMs = -Infinity;

  while (true) {
    const url = `${AC_API_URL}/api/3/deals/${dealId}/notes?limit=${pageSize}&offset=${offset}`;
    const res = await fetch(url, { headers: acHeaders });
    if (!res.ok) {
      throw new Error(`AC GET deal notes -> HTTP ${res.status}: ${await res.text()}`);
    }
    const body = await res.json();
    const notes = (body.notes ?? []) as DealNote[];

    for (const n of notes) {
      const uid = String(n.userid);
      if (uid === JULIENNE_AC_USER_ID || uid === "0") continue;
      const ms = Date.parse(n.cdate);
      if (!Number.isNaN(ms) && ms > latestHumanNoteMs) latestHumanNoteMs = ms;
    }

    offset += notes.length; // advance by actual count, not the requested limit
    const total = Number(body?.meta?.total ?? offset);
    if (notes.length === 0 || offset >= total) break;
  }

  return latestHumanNoteMs > finalMs;
}

async function processLead(
  supabase: SupabaseClient,
  lead: Lead,
  tagId: string,
): Promise<{ error?: { step: string; message: string }; skipped?: boolean }> {
  const nowIso = () => new Date().toISOString();
  const table = lead.source_table;

  if (!lead.deal_id) {
    return { error: { step: "precheck", message: "deal_id is null" } };
  }

  // Skip guard: only evaluated for leads that have not started processing, so a
  // lead already mid-run from a prior partial failure still finishes cleanly.
  if (!lead.unresponsive_note_added_date) {
    let skip: boolean;
    try {
      skip = await hasHumanNoteAfterFinalEmail(lead.deal_id, lead.final_email_date);
    } catch (e) {
      // Could not determine engagement -> do NOT close; retry next run.
      return { error: { step: "skip-check", message: e instanceof Error ? e.message : String(e) } };
    }
    if (skip) {
      const { error: updErr } = await supabase
        .from(table)
        .update({ unresponsive_lead_processing_is_skipped: true })
        .eq("id", lead.source_id);
      if (updErr) {
        return { error: { step: "skip-mark", message: `Supabase update skip flag -> ${updErr.message}` } };
      }
      return { skipped: true };
    }
  }

  // Step 1: add deal note (skip if already added).
  if (!lead.unresponsive_note_added_date) {
    try {
      const noteText =
        `${lead.contact_email} did not respond to any of our emails. ` +
        `Changing deal status to lost and tagging lead as ${TAG_NAME}. ` +
        `This note has been added through a supabase edge function automation.`;
      const res = await fetch(`${AC_API_URL}/api/3/deals/${lead.deal_id}/notes`, {
        method: "POST",
        headers: acHeaders,
        body: JSON.stringify({ note: { note: noteText } }),
      });
      if (!res.ok) throw new Error(`AC POST note -> HTTP ${res.status}: ${await res.text()}`);

      const { error: updErr } = await supabase
        .from(table)
        .update({ unresponsive_note_added_date: nowIso() })
        .eq("id", lead.source_id);
      if (updErr) throw new Error(`Supabase update note date -> ${updErr.message}`);
    } catch (e) {
      return { error: { step: "note", message: e instanceof Error ? e.message : String(e) } };
    }
  }

  // Step 2: tag the contact (skip if already tagged).
  if (!lead.unresponsive_tag_added_date) {
    try {
      if (!lead.contact_id) throw new Error("contact_id is null");
      const res = await fetch(`${AC_API_URL}/api/3/contactTags`, {
        method: "POST",
        headers: acHeaders,
        body: JSON.stringify({ contactTag: { contact: lead.contact_id, tag: tagId } }),
      });
      // AC de-dupes tags; re-applying is safe.
      if (!res.ok) throw new Error(`AC POST contactTag -> HTTP ${res.status}: ${await res.text()}`);

      const { error: updErr } = await supabase
        .from(table)
        .update({ unresponsive_tag_added_date: nowIso() })
        .eq("id", lead.source_id);
      if (updErr) throw new Error(`Supabase update tag date -> ${updErr.message}`);
    } catch (e) {
      return { error: { step: "tag", message: e instanceof Error ? e.message : String(e) } };
    }
  }

  // Step 3: set deal status to Lost -> stamp ac_lost_status_date (done marker).
  try {
    const res = await fetch(`${AC_API_URL}/api/3/deals/${lead.deal_id}`, {
      method: "PUT",
      headers: acHeaders,
      body: JSON.stringify({ deal: { status: AC_STATUS_LOST } }),
    });
    if (!res.ok) throw new Error(`AC PUT deal status -> HTTP ${res.status}: ${await res.text()}`);

    const { error: updErr } = await supabase
      .from(table)
      .update({ ac_lost_status_date: nowIso() })
      .eq("id", lead.source_id);
    if (updErr) throw new Error(`Supabase update ac_lost_status_date -> ${updErr.message}`);
  } catch (e) {
    return { error: { step: "lost", message: e instanceof Error ? e.message : String(e) } };
  }

  return {};
}

async function run(limit?: number): Promise<RunResult> {
  const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });

  const { data, error } = await supabase.rpc("get_unresponsive_lost_leads");
  if (error) throw new Error(`RPC get_unresponsive_lost_leads failed: ${error.message}`);

  let leads = (data ?? []) as Lead[];
  if (limit && limit > 0) leads = leads.slice(0, limit);

  const tagId = await resolveTagId();

  const result: RunResult = {
    ran_at: new Date().toISOString(),
    processed: 0,
    succeeded: 0,
    skipped: 0,
    failed: 0,
    errors: [],
  };

  for (const lead of leads) {
    result.processed++;
    const outcome = await processLead(supabase, lead, tagId);
    if (outcome.error) {
      result.failed++;
      result.errors.push({
        email: lead.contact_email,
        deal: lead.deal_id,
        step: outcome.error.step,
        error: outcome.error.message,
      });
    } else if (outcome.skipped) {
      result.skipped++;
    } else {
      result.succeeded++;
    }
  }

  console.log("process-unresponsive-lost-leads summary:", JSON.stringify(result));
  return result;
}

// Resolve the unresponsive_contact tag id once per run (cached), creating it if
// it does not exist so the function stays robust if the tag is ever removed.
async function resolveTagId(): Promise<string> {
  const searchUrl = `${AC_API_URL}/api/3/tags?search=${encodeURIComponent(TAG_NAME)}`;
  const res = await fetch(searchUrl, { headers: acHeaders });
  if (!res.ok) {
    throw new Error(`AC GET tags -> HTTP ${res.status}: ${await res.text()}`);
  }
  const body = await res.json();
  const match = (body.tags ?? []).find(
    (t: { tag: string; id: string }) => t.tag === TAG_NAME,
  );
  if (match) return String(match.id);

  // Not found -> create it.
  const createRes = await fetch(`${AC_API_URL}/api/3/tags`, {
    method: "POST",
    headers: acHeaders,
    body: JSON.stringify({
      tag: { tag: TAG_NAME, tagType: "contact", description: "" },
    }),
  });
  if (!createRes.ok) {
    throw new Error(`AC POST tag -> HTTP ${createRes.status}: ${await createRes.text()}`);
  }
  const created = await createRes.json();
  return String(created.tag.id);
}

Deno.serve(async (req: Request) => {
  if (!SUPABASE_URL || !SERVICE_ROLE_KEY || !AC_API_URL || !AC_API_TOKEN) {
    return Response.json(
      {
        status: "error",
        error:
          "Missing required environment variables (SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, AC_API_URL, AC_API_TOKEN).",
      },
      { status: 500 },
    );
  }

  const params = new URL(req.url).searchParams;
  const limitParam = params.get("limit");
  const limit = limitParam ? parseInt(limitParam, 10) : undefined;

  // Debug/testing: ?sync=1 awaits the work and returns the outcome (or error)
  // in the response, so failures are visible instead of vanishing in the bg task.
  if (params.get("sync") === "1") {
    try {
      const result = await run(limit);
      return Response.json({ status: "done", ...result });
    } catch (err) {
      console.error(`run() crashed: ${err}`);
      return Response.json(
        { status: "error", error: String(err), stack: (err as Error)?.stack },
        { status: 500 },
      );
    }
  }

  const work = run(limit).catch((err) => console.error(`run() crashed: ${err}`));
  if (typeof EdgeRuntime !== "undefined") {
    EdgeRuntime.waitUntil(work);
  }
  return new Response(JSON.stringify({ status: "started" }), {
    status: 202,
    headers: { "Content-Type": "application/json" },
  });
});
