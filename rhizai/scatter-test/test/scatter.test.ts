/**
 * E2E Test for Scatter Workflow
 *
 * Tests a rhiza workflow with scatter (1:N) fan-out:
 * 1. Scatter worker receives 1 entity, creates 3 copies
 * 2. Stamp worker is invoked 3 times in parallel (once per copy)
 * 3. All 3 copies get stamped
 *
 * Verifies:
 * - Scatter worker creates N copies
 * - Batch entity is created for coordination
 * - N parallel invocations occur
 * - All logs complete successfully
 *
 * Prerequisites:
 * 1. Deploy scatter worker: cd ../../kladoi/scatter-worker && npm run register
 * 2. Deploy stamp worker: cd ../../kladoi/stamp-worker && npm run register
 * 3. Register this workflow: npm run register -- scatter-test
 * 4. Set environment variables (see ../../.env)
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
  apiRequest,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const RHIZA_ID = process.env.RHIZA_ID;
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;
const STAMP_KLADOS = process.env.STAMP_KLADOS;

// Number of copies the scatter worker creates
const NUM_COPIES = 3;

// =============================================================================
// Test Suite
// =============================================================================

describe('scatter workflow', () => {
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
    log(`Using stamp klados: ${STAMP_KLADOS}`);
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) return;

    log('Creating test fixtures...');

    targetCollection = await createCollection({
      label: `Scatter Test Targets ${Date.now()}`,
      description: 'Target collection for scatter workflow test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Test Entity for Scatter',
        content: 'This entity will be copied 3 times, each copy gets stamped',
        created_at: new Date().toISOString(),
      },
      collectionId: targetCollection.id,
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

  it('should scatter to N copies and stamp each in parallel', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the rhiza workflow
    log('Invoking scatter test rhiza...');
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

    // Wait for workflow completion
    // Expecting: 1 scatter log + 3 stamp logs = 4 total
    log('Waiting for workflow completion...');
    const completion = await waitForWorkflowCompletion(jobCollectionId, {
      timeout: 120000, // Longer timeout for parallel execution
      pollInterval: 3000,
      expectedSteps: 1 + NUM_COPIES, // scatter + N stamps
    });

    log(`Workflow status: ${completion.status}`);
    log(`Logs found: ${completion.logs.length}`);

    // Log details for debugging
    for (const logEntry of completion.logs) {
      log(`  - ${logEntry.properties.klados_id}: ${logEntry.properties.status}`);
    }

    // Assert workflow completed successfully
    assertWorkflowCompleted(completion, 1 + NUM_COPIES);

    // Verify we have the right logs:
    // - 1 scatter worker log
    // - N stamp worker logs
    const scatterLogs = completion.logs.filter(
      (l) => l.properties.klados_id === SCATTER_KLADOS
    );
    const stampLogs = completion.logs.filter(
      (l) => l.properties.klados_id === STAMP_KLADOS
    );

    expect(scatterLogs).toHaveLength(1);
    expect(stampLogs).toHaveLength(NUM_COPIES);

    log(`Scatter logs: ${scatterLogs.length}`);
    log(`Stamp logs: ${stampLogs.length}`);

    // Verify a batch entity was created
    const batchResponse = await apiRequest<{ entities: Array<{ pi: string }> }>(
      'GET',
      `/collections/${jobCollectionId}/entities?type=batch`
    );

    log(`Batch entities found: ${batchResponse.entities?.length || 0}`);
    expect(batchResponse.entities?.length).toBeGreaterThanOrEqual(1);

    // Fetch the copies (they're in the job collection)
    const copiesResponse = await apiRequest<{ entities: Array<{ pi: string }> }>(
      'GET',
      `/collections/${jobCollectionId}/entities?type=test_entity`
    );

    const copies = copiesResponse.entities || [];
    log(`Copies found: ${copies.length}`);
    expect(copies.length).toBe(NUM_COPIES);

    // Verify each copy has a stamp
    for (const copy of copies) {
      const entity = await getEntity(copy.pi);
      log(`Copy ${entity.properties.copy_index}: ${entity.properties.label}`);

      // Each copy should have 1 stamp
      const stamps = entity.properties.stamps as Array<{ stamped_by: string }>;
      expect(stamps).toHaveLength(1);
      expect(stamps[0].stamped_by).toBe(STAMP_KLADOS);
    }

    log('✅ Scatter workflow completed successfully!');
    log(`   - Created ${NUM_COPIES} copies`);
    log(`   - Each copy was stamped in parallel`);
  });
});
