/**
 * E2E Test for Sub-Rhiza Invocation
 *
 * Tests that sub-rhiza invocation works correctly after the fix in rhiza 0.6.0.
 *
 * Flow:
 * 1. Invoke parent rhiza (sub-rhiza-test) with test entity having copy_count: 3
 * 2. Scatter klados creates 3 copies
 * 3. Scatter fans out: invokes stamp-chain sub-rhiza 3 times (once per copy)
 * 4. Each stamp-chain stamps the copy twice
 * 5. Result: 3 copies, each with 2 stamps
 *
 * Verifies:
 * - Sub-rhiza invocation via scatter works
 * - target_entity flows correctly to sub-rhiza entry klados
 * - Logs stay in same job_collection (unified observability)
 * - Logs connected via parent_logs (tree traversal works)
 * - Total: 7 logs (1 scatter + 3×2 stamp-chain steps)
 *
 * Prerequisites:
 * 1. stamp-chain rhiza must be registered
 * 2. Register this workflow: npm run register -- sub-rhiza-test
 * 3. Set environment variables (see .env.example)
 *
 * Usage:
 *   source ../.env && RHIZA_ID=<id> npm test
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createCollection,
  createEntity,
  getEntity,
  deleteEntity,
  invokeRhiza,
  waitForWorkflowTree,
  sleep,
  log,
} from '@arke-institute/klados-testing';
import { setupTestClient, hasSubRhizaConfig, RHIZA_ID, STAMP_KLADOS } from './setup.js';

// =============================================================================
// Test Suite
// =============================================================================

describe('sub-rhiza-test workflow', () => {
  // Test fixtures
  let targetCollection: { id: string };
  let jobCollectionId: string;
  let testEntity: { id: string };
  let configValid = false;

  // Skip tests if environment not configured
  beforeAll(() => {
    configValid = setupTestClient() && hasSubRhizaConfig();
    if (!configValid) {
      console.warn('\nTest skipped: Missing required environment variables');
      console.warn('See .env.example for required configuration\n');
    }
  });

  // Create test fixtures
  beforeAll(async () => {
    if (!configValid) return;

    log('Creating test fixtures...');

    // Create target collection
    targetCollection = await createCollection({
      label: `Sub-Rhiza Test Target ${Date.now()}`,
      description: 'Target collection for sub-rhiza invocation test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create test entity with copy_count for scatter
    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        title: 'Test Entity for Sub-Rhiza',
        content: 'This entity will be scattered, copies will go through sub-rhiza',
        copy_count: 3,  // Scatter will create 3 copies
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id} (copy_count: 3)`);
  });

  // Cleanup test fixtures
  afterAll(async () => {
    if (!configValid) return;

    log('Cleaning up test fixtures...');

    try {
      await sleep(1000);

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

  it('should invoke sub-rhiza for each scattered copy', async () => {
    if (!configValid) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the parent workflow
    log('Invoking sub-rhiza-test workflow...');
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID!,
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

    // Wait for workflow completion using tree traversal
    // Expected: 1 scatter + 3 sub-rhiza invocations × 2 stamps each = 7 logs
    log('Waiting for workflow tree completion...');
    const tree = await waitForWorkflowTree(jobCollectionId, {
      timeout: 180000,  // 3 minutes - sub-rhiza adds latency
      pollInterval: 5000,
      onPoll: (t, elapsed) => {
        log(`  Poll at ${Math.round(elapsed / 1000)}s: ${t.logs.size} logs, complete=${t.isComplete}`);
      },
    });

    log(`Workflow tree: ${tree.logs.size} logs, complete=${tree.isComplete}`);

    // Verify workflow completed
    expect(tree.isComplete).toBe(true);

    // Should have 7 logs total: 1 scatter + 3 × 2 stamps
    expect(tree.logs.size).toBe(7);

    // All logs should be in the same job collection (unified observability)
    const allLogs = Array.from(tree.logs.values());
    log('\nLog summary:');
    for (const logEntry of allLogs) {
      const props = logEntry.properties;
      log(`  ${props.klados_id?.slice(0, 12)}... - status: ${props.status}, rhiza: ${props.rhiza_id?.slice(0, 12)}...`);
    }

    // Count logs by type
    const scatterLogs = allLogs.filter(l =>
      l.properties.klados_id === process.env.SCATTER_KLADOS
    );
    const stampLogs = allLogs.filter(l =>
      l.properties.klados_id === STAMP_KLADOS
    );

    log(`\nScatter logs: ${scatterLogs.length}`);
    log(`Stamp logs: ${stampLogs.length}`);

    // Verify all logs completed successfully
    for (const logEntry of allLogs) {
      expect(logEntry.properties.status).toBe('done');
    }

    // Verify entity relationships: original should have has_copy relationships
    log('\nVerifying entity relationships...');
    const originalEntity = await getEntity(testEntity.id);
    const copyRels = originalEntity.relationships?.filter(r => r.predicate === 'has_copy') ?? [];

    log(`Original entity has ${copyRels.length} copy relationships`);
    expect(copyRels.length).toBe(3);

    // Verify each copy has 2 stamps (from stamp-chain sub-rhiza)
    log('\nVerifying stamps on copies...');
    for (const rel of copyRels) {
      const copy = await getEntity(rel.peer);
      const stamps = copy.properties.stamps as Array<{ stamp_number: number }> | undefined;

      log(`  Copy ${rel.peer.slice(0, 12)}...: ${stamps?.length ?? 0} stamps`);

      expect(stamps).toBeDefined();
      expect(stamps).toHaveLength(2);
      expect(stamps![0].stamp_number).toBe(1);
      expect(stamps![1].stamp_number).toBe(2);
    }

    log('\n✅ Sub-rhiza invocation test passed!');
    log(`  - Scatter created 3 copies`);
    log(`  - Each copy processed by stamp-chain sub-rhiza`);
    log(`  - Each copy has 2 stamps`);
    log(`  - All 7 logs in unified job collection`);
    log(`  - Tree traversal found all logs`);
  }, 200000);  // 200s timeout for the test
});
