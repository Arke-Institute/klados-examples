/**
 * Klados Worker - Lightweight worker template for Arke rhiza workflows
 *
 * This template provides a minimal klados worker that:
 * 1. Accepts job requests from Arke
 * 2. Processes a target entity
 * 3. Creates output entities
 * 4. Hands off to the next step in the workflow
 *
 * The worker uses KladosJob from @arke-institute/rhiza which handles:
 * - Log entry creation and finalization
 * - Error handling (both log and batch slot updates)
 * - Workflow handoff via interpretThen
 */

import { Hono } from 'hono';
import { KladosJob, type KladosRequest } from '@arke-institute/rhiza';
import { processJob } from './job';
import type { Env } from './types';

const app = new Hono<{ Bindings: Env }>();

/**
 * Health check endpoint
 */
app.get('/health', (c) => {
  return c.json({
    status: 'ok',
    agent_id: c.env.AGENT_ID,
    version: c.env.AGENT_VERSION,
  });
});

/**
 * Arke verification endpoint
 * Required to verify ownership of this endpoint before activating the klados.
 * Returns the verification token provided during registration.
 *
 * Uses ARKE_VERIFY_AGENT_ID during initial verification (before AGENT_ID is set),
 * then falls back to AGENT_ID for subsequent verifications.
 */
app.get('/.well-known/arke-verification', (c) => {
  const token = c.env.VERIFICATION_TOKEN;
  // Use verification-specific agent ID if set, otherwise fall back to main AGENT_ID
  const kladosId = c.env.ARKE_VERIFY_AGENT_ID || c.env.AGENT_ID;

  if (!token || !kladosId) {
    return c.json({ error: 'Verification not configured' }, 500);
  }

  return c.json({
    verification_token: token,
    klados_id: kladosId,
  });
});

/**
 * Main job processing endpoint
 * The API calls POST /process to invoke the klados
 */
app.post('/process', async (c) => {
  const req = await c.req.json<KladosRequest>();

  // Accept the job immediately
  const job = KladosJob.accept(req, {
    agentId: c.env.AGENT_ID,
    agentVersion: c.env.AGENT_VERSION,
    authToken: c.env.ARKE_AGENT_KEY,
  });

  // Process in background - KladosJob handles:
  // - Writing initial log entry
  // - Catching errors and updating log + batch slot
  // - Executing workflow handoffs
  // - Finalizing log on completion
  c.executionCtx.waitUntil(
    job.run(async () => {
      return await processJob(job);
    })
  );

  // Return acceptance immediately
  return c.json(job.acceptResponse);
});

export default app;
