/**
 * E2E Test for Nested Scatter Workflow
 *
 * Tests a rhiza workflow with nested scatters (scatter → scatter → stamp):
 * 1. First scatter creates 3 copies from the original entity
 * 2. Each of those 3 copies triggers a second scatter, creating 3 more copies each
 * 3. Each of the 9 final copies gets stamped
 *
 * Expected tree structure:
 * Root scatter (1 log) - expectedChildren=3
 * ├── Scatter 2a (1 log) - expectedChildren=3
 * │   ├── Stamp (1 log)
 * │   ├── Stamp (1 log)
 * │   └── Stamp (1 log)
 * ├── Scatter 2b (1 log) - expectedChildren=3
 * │   ├── Stamp (1 log)
 * │   ├── Stamp (1 log)
 * │   └── Stamp (1 log)
 * └── Scatter 2c (1 log) - expectedChildren=3
 *     ├── Stamp (1 log)
 *     ├── Stamp (1 log)
 *     └── Stamp (1 log)
 *
 * Total: 1 + 3 + 9 = 13 logs
 *
 * Prerequisites:
 * 1. Deploy scatter worker: cd ../../kladoi/scatter-worker && npm run register
 * 2. Deploy stamp worker: cd ../../kladoi/stamp-worker && npm run register
 * 3. Register this workflow: npm run register -- nested-scatter-test
 * 4. Set environment variables (see ../../.env)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  invokeRhiza,
  waitForWorkflowTree,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const NESTED_SCATTER_RHIZA = process.env.NESTED_SCATTER_RHIZA;
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;
const STAMP_KLADOS = process.env.STAMP_KLADOS;

// =============================================================================
// Test Suite
// =============================================================================

describe('nested scatter workflow', () => {
  let targetCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string;

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.error('Skipping tests: ARKE_USER_KEY not set');
      return;
    }

    if (!NESTED_SCATTER_RHIZA) {
      console.error('Skipping tests: NESTED_SCATTER_RHIZA not set');
      console.error('Run: npm run register -- nested-scatter-test');
      return;
    }

    log(`Using rhiza: ${NESTED_SCATTER_RHIZA}`);
    log(`Using scatter klados: ${SCATTER_KLADOS}`);
    log(`Using stamp klados: ${STAMP_KLADOS}`);

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !NESTED_SCATTER_RHIZA) return;

    log('Creating test fixtures...');

    // Create target collection
    targetCollection = await createCollection({ label: 'Nested Scatter Test' });
    log(`Created target collection: ${targetCollection.id}`);

    // Create test entity with copy_count: 3
    // Each scatter level will create 3 copies
    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Nested Scatter Source',
        copy_count: 3,  // Each scatter creates 3 copies
      },
      collectionId: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id} with copy_count: 3`);
  });

  afterAll(async () => {
    if (jobCollectionId) {
      log('Cleanup DISABLED for inspection');
      log(`  Target collection: ${targetCollection?.id}`);
      log(`  Test entity: ${testEntity?.id}`);
      log(`  Job collection: ${jobCollectionId}`);
    }
  });

  it('should handle nested scatter (scatter → scatter → stamp)', async () => {
    if (!ARKE_USER_KEY || !NESTED_SCATTER_RHIZA) {
      console.error('Test skipped: missing environment variables');
      return;
    }

    log('Invoking nested scatter workflow...');

    // Invoke nested scatter workflow
    const result = await invokeRhiza({
      rhizaId: NESTED_SCATTER_RHIZA,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_collection).toBeDefined();

    jobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);
    log('Waiting for workflow tree to complete...');

    // Wait for workflow tree to complete
    // Expected: 1 root scatter + 3 level-2 scatters + 9 stamps = 13 logs
    const tree = await waitForWorkflowTree(jobCollectionId, {
      timeout: 180000,
      pollInterval: 3000,
      onPoll: (t, elapsed) => {
        log(`[${Math.round(elapsed / 1000)}s] logs=${t.logs.size}, complete=${t.isComplete}, allChildrenDiscovered=${t.allChildrenDiscovered}`);
      },
    });

    log('Workflow tree result:');
    log(`  isComplete: ${tree.isComplete}`);
    log(`  allChildrenDiscovered: ${tree.allChildrenDiscovered}`);
    log(`  logs.size: ${tree.logs.size}`);
    log(`  leaves.length: ${tree.leaves.length}`);
    log(`  hasErrors: ${tree.hasErrors}`);

    // Verify tree structure
    expect(tree.isComplete).toBe(true);
    expect(tree.allChildrenDiscovered).toBe(true);
    expect(tree.logs.size).toBe(13);  // 1 + 3 + 9
    expect(tree.hasErrors).toBe(false);

    // Verify root node
    expect(tree.root).toBeDefined();
    log(`Root node: expectedChildren=${tree.root!.expectedChildren}, actual=${tree.root!.children.length}`);
    expect(tree.root!.expectedChildren).toBe(3);
    expect(tree.root!.children.length).toBe(3);

    // Verify level-2 scatter nodes (each should have 3 stamp children)
    for (let i = 0; i < tree.root!.children.length; i++) {
      const level2 = tree.root!.children[i];
      log(`Level-2 scatter ${i}: expectedChildren=${level2.expectedChildren}, actual=${level2.children.length}`);

      expect(level2.log.properties.klados_id).toBe(SCATTER_KLADOS);
      expect(level2.expectedChildren).toBe(3);
      expect(level2.children.length).toBe(3);

      // Verify stamps (leaves)
      for (const stamp of level2.children) {
        if (STAMP_KLADOS) {
          expect(stamp.log.properties.klados_id).toBe(STAMP_KLADOS);
        }
        expect(stamp.isLeaf).toBe(true);
        expect(stamp.isTerminal).toBe(true);
        expect(stamp.expectedChildren).toBe(0);
      }
    }

    // Verify leaves count
    expect(tree.leaves.length).toBe(9);  // All stamps are leaves
    log('All assertions passed!');
  }, 240000);
});
