/**
 * E2E Test for Stamp Chain Workflow
 *
 * Tests a rhiza workflow that chains the same stamp klados twice.
 * Verifies that:
 * 1. The workflow invokes correctly
 * 2. Both steps execute in sequence
 * 3. Stamps accumulate on the entity (2 stamps)
 * 4. Logs show correct workflow path
 *
 * Prerequisites:
 * 1. Deploy stamp worker: cd ../stamp-worker && npm run register
 * 2. Register this workflow: npm run register -- stamp-chain
 * 3. Set environment variables (see ../.env)
 *
 * Usage:
 *   source ../.env && RHIZA_ID=rhiza_xxx npm test
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  getEntity,
  deleteEntity,
  invokeRhiza,
  waitForWorkflowCompletion,
  assertWorkflowCompleted,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const RHIZA_ID = process.env.RHIZA_ID;
const STAMP_KLADOS = process.env.STAMP_KLADOS;

// =============================================================================
// Test Suite
// =============================================================================

describe('stamp-chain workflow', () => {
  let targetCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string; // Returned from API invoke

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!RHIZA_ID) {
      console.warn('Skipping tests: RHIZA_ID not set');
      console.warn('Run: npm run register -- stamp-chain');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });

    log(`Using rhiza: ${RHIZA_ID}`);
    log(`Using stamp klados: ${STAMP_KLADOS}`);
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) return;

    log('Creating test fixtures...');

    targetCollection = await createCollection({
      label: `Stamp Chain Targets ${Date.now()}`,
      description: 'Target collection for stamp chain workflow test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        title: 'Test Entity for Stamp Chain',
        content: 'This entity will be stamped twice by the same klados',
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) return;

    log('Cleaning up test fixtures...');
    try {
      if (testEntity?.id) await deleteEntity(testEntity.id);
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      if (jobCollectionId) await deleteEntity(jobCollectionId);
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should stamp entity twice using same klados in chain', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the rhiza workflow (API creates job collection)
    log('Invoking stamp chain rhiza...');
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    // Store job collection ID for cleanup and polling
    jobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);

    // Wait for workflow completion (2 steps)
    log('Waiting for workflow completion...');
    const completion = await waitForWorkflowCompletion(jobCollectionId, {
      timeout: 90000,
      pollInterval: 3000,
      expectedSteps: 2,
    });

    log(`Workflow status: ${completion.status}`);
    log(`Logs found: ${completion.logs.length}`);

    // Assert workflow completed successfully
    assertWorkflowCompleted(completion, 2);

    // Verify the entity has 2 stamps
    const finalEntity = await getEntity(testEntity.id);
    const stamps = finalEntity.properties.stamps as Array<{
      stamp_number: number;
      stamped_by: string;
      stamp_message: string;
    }>;
    log(`Final entity stamps: ${JSON.stringify(stamps, null, 2)}`);

    expect(stamps).toHaveLength(2);
    expect(finalEntity.properties.stamp_count).toBe(2);

    // Verify stamp order
    expect(stamps[0].stamp_number).toBe(1);
    expect(stamps[1].stamp_number).toBe(2);

    // Both stamps should be from the same klados (but different jobs)
    expect(stamps[0].stamped_by).toBe(STAMP_KLADOS);
    expect(stamps[1].stamped_by).toBe(STAMP_KLADOS);

    log('✅ Stamp chain workflow completed successfully!');
    log(`   - First stamp: ${stamps[0].stamp_message}`);
    log(`   - Second stamp: ${stamps[1].stamp_message}`);
  });
});
