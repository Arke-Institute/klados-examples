/**
 * E2E Test for Scatter-Utility Delegation
 *
 * Tests that large scatters (>50 outputs) are automatically delegated
 * to the scatter-utility service.
 *
 * The scatter worker now uses the local rhiza package which has
 * scatter-utility delegation built in as the default behavior.
 *
 * This test:
 * 1. Creates an entity with copy_count: 100 (above threshold)
 * 2. Invokes the scatter workflow
 * 3. Verifies the scatter was delegated to scatter-utility
 * 4. Waits for all items to be dispatched
 *
 * Prerequisites:
 * 1. Deploy scatter worker with local rhiza: cd ../../kladoi/scatter-worker && npm run deploy
 * 2. Deploy stamp worker: cd ../../kladoi/stamp-worker && npm run deploy
 * 3. Register this workflow: npm run register -- scatter-test
 * 4. Set environment variables (see ../../.env)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  deleteEntity,
  invokeRhiza,
  waitForWorkflowCompletion,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const RHIZA_ID = process.env.RHIZA_ID;
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;
const SCATTER_UTILITY_URL = process.env.SCATTER_UTILITY_URL || 'https://scatter-utility.arke.institute';

// Number of copies to trigger scatter-utility delegation (must be > 50)
// Using 51 - minimum above threshold
const LARGE_COPY_COUNT = 51;

// =============================================================================
// Test Suite
// =============================================================================

describe('scatter-utility delegation', () => {
  let targetCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string;

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!RHIZA_ID) {
      console.warn('Skipping tests: RHIZA_ID not set');
      console.warn('Run: npm run register -- scatter-test');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });

    log(`Using rhiza: ${RHIZA_ID}`);
    log(`Using scatter klados: ${SCATTER_KLADOS}`);
    log(`Scatter utility: ${SCATTER_UTILITY_URL}`);
    log(`Copy count: ${LARGE_COPY_COUNT} (should trigger delegation)`);
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) return;

    log('Creating test fixtures...');

    targetCollection = await createCollection({
      label: `Scatter Utility Test ${Date.now()}`,
      description: 'Target collection for scatter-utility delegation test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create entity with copy_count to trigger large scatter
    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Test Entity for Scatter-Utility',
        content: `This entity will create ${LARGE_COPY_COUNT} copies via scatter-utility`,
        copy_count: LARGE_COPY_COUNT,  // This triggers the large scatter
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id} with copy_count: ${LARGE_COPY_COUNT}`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) return;

    // Cleanup
    log('Cleaning up...');
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

  it('should delegate large scatter to scatter-utility', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the rhiza workflow
    log('Invoking scatter workflow with large copy count...');
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    jobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);

    // Wait for the scatter step to complete
    // The scatter worker should:
    // 1. Create 100 copies
    // 2. Delegate to scatter-utility (because 100 > 50 threshold)
    // 3. Complete its log
    log('Waiting for scatter step to complete...');

    // First, wait for just the scatter log to appear and complete
    const scatterCompletion = await waitForWorkflowCompletion(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
      expectedSteps: 1, // Just the scatter step
    });

    log(`Scatter step status: ${scatterCompletion.status}`);
    log(`Logs found: ${scatterCompletion.logs.length}`);

    // Find the scatter log
    const scatterLog = scatterCompletion.logs.find(
      (l) => l.properties.klados_id === SCATTER_KLADOS
    );

    expect(scatterLog).toBeDefined();
    expect(scatterLog?.properties.status).toBe('done');

    log('Scatter log details:');
    log(`  Status: ${scatterLog?.properties.status}`);
    log(`  Log ID: ${scatterLog?.id}`);

    // Check the handoff record for delegation info
    const handoffs = scatterLog?.properties.log_data?.entry?.handoffs as Array<{
      type: string;
      delegated?: boolean;
      dispatch_id?: string;
    }> | undefined;

    log(`  Handoffs: ${JSON.stringify(handoffs, null, 2)}`);

    // Verify delegation happened
    const scatterHandoff = handoffs?.find(h => h.type === 'scatter');
    expect(scatterHandoff).toBeDefined();
    expect(scatterHandoff?.delegated).toBe(true);
    expect(scatterHandoff?.dispatch_id).toBeDefined();

    const dispatchId = scatterHandoff?.dispatch_id;
    log(`✅ Scatter was delegated to scatter-utility!`);
    log(`   Dispatch ID: ${dispatchId}`);

    // Poll scatter-utility for completion
    if (dispatchId) {
      log('Polling scatter-utility for dispatch completion...');
      let complete = false;

      for (let i = 0; i < 120; i++) {
        const statusRes = await fetch(`${SCATTER_UTILITY_URL}/status/${dispatchId}`);
        if (!statusRes.ok) {
          log(`  Status check failed: ${statusRes.status}`);
          break;
        }

        const status = await statusRes.json() as {
          status: string;
          total: number;
          dispatched: number;
          failed: number;
        };

        log(`  Progress: ${status.dispatched}/${status.total} dispatched, ${status.failed} failed, status: ${status.status}`);

        if (status.status === 'complete') {
          log(`✅ Scatter-utility dispatch complete!`);
          log(`   Total: ${status.total}`);
          log(`   Dispatched: ${status.dispatched}`);
          log(`   Failed: ${status.failed}`);
          complete = true;

          // Verify counts
          expect(status.total).toBe(LARGE_COPY_COUNT);
          break;
        }

        if (status.status === 'error') {
          throw new Error(`Scatter-utility error: ${JSON.stringify(status)}`);
        }

        await new Promise(resolve => setTimeout(resolve, 2000));
      }

      expect(complete).toBe(true);
    }

    log('');
    log('=== Scatter-Utility E2E Test Complete ===');
    log(`   - Created entity with copy_count: ${LARGE_COPY_COUNT}`);
    log(`   - Scatter worker delegated to scatter-utility`);
    log(`   - All ${LARGE_COPY_COUNT} dispatches completed`);
  }, 300000); // 5 minute timeout for this test
});
