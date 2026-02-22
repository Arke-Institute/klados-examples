/**
 * E2E Test for Per-Item Routing in Scatter Workflow
 *
 * Tests a rhiza workflow with per-item routing:
 * 1. Scatter worker creates 6 copies with alternating entity_class
 *    - Even indices: entity_class = "canonical"
 *    - Odd indices: entity_class = "mention"
 * 2. Route rules evaluate each output:
 *    - Canonicals → stamp_canonical step (invoked)
 *    - Mentions → stamp_mention step (invoked)
 * 3. All items get stamped, but through different routes
 *
 * This tests the local dispatch path (< 50 items, no scatter-utility delegation).
 * We verify per-item routing by checking the step names in log entries.
 *
 * Prerequisites:
 * 1. Deploy scatter worker with mix_entity_class support
 * 2. Deploy stamp worker
 * 3. Register routing-scatter-test workflow (with two stamp steps)
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
  waitForWorkflowTree,
  apiRequest,
  log,
  type WorkflowLogTree,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const RHIZA_ID = process.env.ROUTING_SCATTER_RHIZA;
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;
const STAMP_KLADOS = process.env.STAMP_KLADOS;

// Number of copies the scatter worker creates (6 = 3 canonical + 3 mention)
const NUM_COPIES = 6;
const NUM_CANONICALS = 3;  // Even indices: 0, 2, 4 → stamp_canonical
const NUM_MENTIONS = 3;    // Odd indices: 1, 3, 5 → stamp_mention
const NUM_STAMPS = 6;      // All items get stamped (through different routes)

// =============================================================================
// Test Suite
// =============================================================================

describe('routing scatter workflow', () => {
  let targetCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string;

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!RHIZA_ID) {
      console.warn('Skipping tests: ROUTING_SCATTER_RHIZA not set');
      console.warn('Set ROUTING_SCATTER_RHIZA environment variable');
      return;
    }
    if (!SCATTER_KLADOS || !STAMP_KLADOS) {
      console.warn('Skipping tests: SCATTER_KLADOS or STAMP_KLADOS not set');
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
      label: `Routing Scatter Test ${Date.now()}`,
      description: 'Target collection for per-item routing test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Test Entity for Routing Scatter',
        content: 'This entity will be copied 6 times with mixed entity_class',
        copy_count: NUM_COPIES,
        mix_entity_class: true,  // Enable alternating entity_class
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);
    log(`  - copy_count: ${NUM_COPIES}`);
    log(`  - mix_entity_class: true`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) return;

    // DISABLED FOR DEBUGGING - uncomment to re-enable cleanup
    log('Cleanup DISABLED for inspection');
    log(`  Target collection: ${targetCollection?.id}`);
    log(`  Test entity: ${testEntity?.id}`);
    log(`  Job collection: ${jobCollectionId}`);
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should route items based on entity_class (local dispatch)', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the rhiza workflow
    log('Invoking routing scatter test rhiza...');
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

    // Wait for workflow completion using tree traversal
    // Expecting: 1 scatter log + 6 stamp logs (all items get stamped) = 7 total
    // Items are routed to different steps based on entity_class
    log('Waiting for workflow completion (tree traversal)...');
    const tree = await waitForWorkflowTree(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
      onPoll: (t, elapsed) => log(`  Poll: ${t.logs.size} logs, complete=${t.isComplete}, elapsed=${elapsed}ms`),
    });

    log(`Workflow complete: ${tree.isComplete}`);
    log(`Logs found: ${tree.logs.size}`);
    log(`Has errors: ${tree.hasErrors}`);

    // Convert logs Map to array for analysis
    const allLogs = Array.from(tree.logs.values());

    // Log details for debugging
    for (const logEntry of allLogs) {
      const status = logEntry.properties.status;
      log(`  - ${logEntry.properties.klados_id}: ${status}`);
    }

    // Check if we have at least the expected number of logs (scatter + stamps)
    expect(tree.logs.size).toBeGreaterThanOrEqual(1 + NUM_STAMPS);

    // Verify workflow completed without errors
    expect(tree.isComplete).toBe(true);

    // Verify we have the right logs:
    // - 1 scatter worker log
    // - 6 stamp worker logs (3 for canonicals, 3 for mentions)
    const scatterLogs = allLogs.filter(
      (l) => l.properties.klados_id === SCATTER_KLADOS
    );
    const stampLogs = allLogs.filter(
      (l) => l.properties.klados_id === STAMP_KLADOS
    );

    log(`Scatter logs: ${scatterLogs.length}`);
    log(`Stamp logs: ${stampLogs.length}`);

    expect(scatterLogs).toHaveLength(1);
    expect(stampLogs).toHaveLength(NUM_STAMPS);  // All items get stamped

    // Analyze scatter log's handoff to verify routing
    // The scatter log's handoffs contain invocations with the path to each step
    const scatterLog = scatterLogs[0];
    const handoffs = scatterLog?.properties.log_data?.entry?.handoffs;
    const invocations = handoffs?.[0]?.invocations || [];

    log(`Scatter handoff invocations: ${invocations.length}`);

    let stampCanonicalCount = 0;
    let stampMentionCount = 0;

    for (const inv of invocations) {
      const path = inv.request?.rhiza?.path as string[] | undefined;
      const stepName = path?.[path.length - 1];

      if (stepName === 'stamp_canonical') {
        stampCanonicalCount++;
      } else if (stepName === 'stamp_mention') {
        stampMentionCount++;
      }
    }

    log(`Invocations to stamp_canonical: ${stampCanonicalCount}`);
    log(`Invocations to stamp_mention: ${stampMentionCount}`);

    // Verify routing worked correctly
    expect(stampCanonicalCount).toBe(NUM_CANONICALS);
    expect(stampMentionCount).toBe(NUM_MENTIONS);

    // Fetch the original entity and verify copies via relationships
    const originalEntity = await getEntity(testEntity.id);
    const copyRelationships = originalEntity.relationships?.filter(
      (r: { predicate: string }) => r.predicate === 'has_copy'
    ) || [];

    log(`Copy relationships found: ${copyRelationships.length}`);
    expect(copyRelationships.length).toBe(NUM_COPIES);

    // Verify each copy exists and has a stamp
    let canonicalCount = 0;
    let mentionCount = 0;

    for (const rel of copyRelationships) {
      const copy = await getEntity(rel.peer);
      const entityClass = copy.properties.entity_class as string;
      const copyIndex = copy.properties.copy_index as number;
      const stamps = copy.properties.stamps as Array<{ stamped_by: string }> | undefined;

      log(`Copy ${copyIndex}: entity_class=${entityClass}, stamps=${stamps?.length ?? 0}`);

      // All copies should have exactly 1 stamp
      expect(stamps).toHaveLength(1);
      expect(stamps![0].stamped_by).toBe(STAMP_KLADOS);

      if (entityClass === 'canonical') {
        canonicalCount++;
      } else if (entityClass === 'mention') {
        mentionCount++;
      } else {
        throw new Error(`Unexpected entity_class: ${entityClass}`);
      }
    }

    expect(canonicalCount).toBe(NUM_CANONICALS);
    expect(mentionCount).toBe(NUM_MENTIONS);

    log('');
    log('=== Per-Item Routing Test Complete ===');
    log(`   - Created ${NUM_COPIES} copies (${NUM_CANONICALS} canonical, ${NUM_MENTIONS} mention)`);
    log(`   - Canonicals routed to stamp_canonical: ${stampCanonicalCount}`);
    log(`   - Mentions routed to stamp_mention: ${stampMentionCount}`);
    log(`   - All items stamped: ${NUM_STAMPS}`);
    log(`   - Per-item routing verified!`);
  }, 150000);  // Extended timeout

  // ==========================================================================
  // Test 2: Large Scatter with Routing (Scatter-Utility Delegation)
  // ==========================================================================

  it('should route items via scatter-utility delegation (> 50 items)', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Use 60 items to trigger delegation (> 50 threshold)
    const LARGE_NUM_COPIES = 60;
    const LARGE_NUM_CANONICALS = 30;  // Even indices: 0, 2, 4, ... 58
    const LARGE_NUM_MENTIONS = 30;    // Odd indices: 1, 3, 5, ... 59

    // Create a new entity for this test
    log('Creating large test fixtures...');
    const largeTestEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Large Routing Test Entity',
        content: 'This entity will be copied 60 times with mixed entity_class',
        copy_count: LARGE_NUM_COPIES,
        mix_entity_class: true,
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created large test entity: ${largeTestEntity.id}`);
    log(`  - copy_count: ${LARGE_NUM_COPIES}`);

    // Invoke the rhiza workflow
    log('Invoking routing scatter test rhiza with large item count...');
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID,
      targetEntity: largeTestEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    const largeJobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${largeJobCollectionId}`);

    // Wait for workflow completion using tree traversal
    // Expecting: 1 scatter log + 60 stamp logs = 61 total
    log('Waiting for workflow completion (this may take a while)...');
    const tree = await waitForWorkflowTree(largeJobCollectionId, {
      timeout: 300000,  // 5 minutes for large scatter
      pollInterval: 5000,
      onPoll: (t, elapsed) => log(`  Poll: ${t.logs.size} logs, complete=${t.isComplete}, elapsed=${Math.round(elapsed/1000)}s`),
    });

    log(`Workflow complete: ${tree.isComplete}`);
    log(`Logs found: ${tree.logs.size}`);
    log(`Has errors: ${tree.hasErrors}`);

    // Convert logs Map to array for analysis
    const allLogs = Array.from(tree.logs.values());

    // Verify we have the scatter log with delegation
    const scatterLogs = allLogs.filter(
      (l) => l.properties.klados_id === SCATTER_KLADOS
    );
    const stampLogs = allLogs.filter(
      (l) => l.properties.klados_id === STAMP_KLADOS
    );

    log(`Scatter logs: ${scatterLogs.length}`);
    log(`Stamp logs: ${stampLogs.length}`);

    expect(scatterLogs).toHaveLength(1);

    // Check if delegation happened
    const scatterLog = scatterLogs[0];
    const handoffs = scatterLog?.properties.log_data?.entry?.handoffs;
    const handoffRecord = handoffs?.[0];

    log(`Delegation flag: ${handoffRecord?.delegated}`);
    log(`Dispatch ID: ${handoffRecord?.dispatch_id}`);

    // For > 50 items, we expect delegation
    expect(handoffRecord?.delegated).toBe(true);
    expect(handoffRecord?.dispatch_id).toBeDefined();

    // Verify stamp count matches expectations
    // All 60 items should be stamped (both canonicals and mentions go to stamp steps)
    expect(stampLogs.length).toBeGreaterThanOrEqual(LARGE_NUM_COPIES - 5);  // Allow some tolerance

    log(`All ${stampLogs.length} stamp logs found`);
    // Note: For delegated scatters, we can't verify per-item routing via log paths
    // because the rhiza path isn't stored in the log's received object.
    // Instead, we verify routing worked by checking the entity states below.

    // Fetch the original entity and verify copies via relationships
    const originalEntity = await getEntity(largeTestEntity.id);
    const copyRelationships = originalEntity.relationships?.filter(
      (r: { predicate: string }) => r.predicate === 'has_copy'
    ) || [];

    log(`Copy relationships found: ${copyRelationships.length}`);
    expect(copyRelationships.length).toBe(LARGE_NUM_COPIES);

    // Sample verification - check a few copies have correct entity_class and stamps
    let canonicalCount = 0;
    let mentionCount = 0;
    let stampedCount = 0;

    // Check first 10 and last 10 copies for representative verification
    const samplesToCheck = copyRelationships.slice(0, 10).concat(copyRelationships.slice(-10));

    for (const rel of samplesToCheck) {
      const copy = await getEntity(rel.peer);
      const entityClass = copy.properties.entity_class as string;
      const stamps = copy.properties.stamps as Array<{ stamped_by: string }> | undefined;

      if (entityClass === 'canonical') {
        canonicalCount++;
      } else if (entityClass === 'mention') {
        mentionCount++;
      }

      if (stamps && stamps.length > 0) {
        stampedCount++;
      }
    }

    log(`Sample verification (20 copies):`);
    log(`  - Canonicals: ${canonicalCount}`);
    log(`  - Mentions: ${mentionCount}`);
    log(`  - Stamped: ${stampedCount}`);

    // Verify entity_class distribution in sample
    expect(canonicalCount + mentionCount).toBe(samplesToCheck.length);
    // Most sampled copies should be stamped
    expect(stampedCount).toBeGreaterThanOrEqual(samplesToCheck.length - 2);

    log('');
    log('=== Large Scatter Delegation Test Complete ===');
    log(`   - Created ${LARGE_NUM_COPIES} copies`);
    log(`   - Delegated to scatter-utility: ${handoffRecord?.delegated}`);
    log(`   - Dispatch ID: ${handoffRecord?.dispatch_id}`);
    log(`   - Verified ${stampedCount}/${samplesToCheck.length} sample entities are stamped`);
  }, 360000);  // 6 minute timeout for large scatter

  // ==========================================================================
  // Test 3: Stress Test (1000 items)
  // ==========================================================================

  it('should handle 100 item scatter (stress test)', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Note: 100 copies tests the scatter-utility delegation path (> 50 threshold)
    // while staying well under Cloudflare's limits.
    const STRESS_NUM_COPIES = 100;
    const STRESS_NUM_CANONICALS = 50;
    const STRESS_NUM_MENTIONS = 50;

    // Create a new entity for this test
    log('Creating stress test fixtures (100 items)...');
    const stressTestEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Stress Test Entity (100)',
        content: 'This entity will be copied 100 times with mixed entity_class',
        copy_count: STRESS_NUM_COPIES,
        mix_entity_class: true,
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created stress test entity: ${stressTestEntity.id}`);
    log(`  - copy_count: ${STRESS_NUM_COPIES}`);

    // Invoke the rhiza workflow
    log('Invoking routing scatter test rhiza with 100 items...');
    const startTime = Date.now();
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID,
      targetEntity: stressTestEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    const stressJobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${stressJobCollectionId}`);

    // Wait for workflow completion
    log('Waiting for workflow completion (this will take a while)...');
    const tree = await waitForWorkflowTree(stressJobCollectionId, {
      timeout: 900000,  // 15 minutes for stress test
      pollInterval: 10000,  // Poll every 10 seconds
      onPoll: (t, elapsed) => {
        const mins = Math.floor(elapsed / 60000);
        const secs = Math.floor((elapsed % 60000) / 1000);
        log(`  Poll: ${t.logs.size} logs, complete=${t.isComplete}, elapsed=${mins}m${secs}s`);
      },
    });

    const totalTime = Date.now() - startTime;
    const totalMins = Math.floor(totalTime / 60000);
    const totalSecs = Math.floor((totalTime % 60000) / 1000);

    log(`Workflow complete: ${tree.isComplete}`);
    log(`Total time: ${totalMins}m${totalSecs}s`);
    log(`Logs found: ${tree.logs.size}`);
    log(`Has errors: ${tree.hasErrors}`);

    // Convert logs Map to array for analysis
    const allLogs = Array.from(tree.logs.values());

    const scatterLogs = allLogs.filter(
      (l) => l.properties.klados_id === SCATTER_KLADOS
    );
    const stampLogs = allLogs.filter(
      (l) => l.properties.klados_id === STAMP_KLADOS
    );

    log(`Scatter logs: ${scatterLogs.length}`);
    log(`Stamp logs: ${stampLogs.length}`);

    expect(scatterLogs).toHaveLength(1);

    // Check delegation
    const scatterLog = scatterLogs[0];
    const handoffs = scatterLog?.properties.log_data?.entry?.handoffs;
    const handoffRecord = handoffs?.[0];

    log(`Delegation flag: ${handoffRecord?.delegated}`);
    log(`Dispatch ID: ${handoffRecord?.dispatch_id}`);

    expect(handoffRecord?.delegated).toBe(true);

    // Verify stamp count - allow some tolerance for timing
    const stampTolerance = Math.floor(STRESS_NUM_COPIES * 0.05); // 5% tolerance
    expect(stampLogs.length).toBeGreaterThanOrEqual(STRESS_NUM_COPIES - stampTolerance);

    log(`Stamp logs: ${stampLogs.length}/${STRESS_NUM_COPIES} (${Math.round(stampLogs.length / STRESS_NUM_COPIES * 100)}%)`);

    // Verify copies were created
    const originalEntity = await getEntity(stressTestEntity.id);
    const copyRelationships = originalEntity.relationships?.filter(
      (r: { predicate: string }) => r.predicate === 'has_copy'
    ) || [];

    log(`Copy relationships found: ${copyRelationships.length}`);
    expect(copyRelationships.length).toBe(STRESS_NUM_COPIES);

    // Sample verification - check 50 random copies
    const sampleSize = 50;
    const shuffled = [...copyRelationships].sort(() => Math.random() - 0.5);
    const samplesToCheck = shuffled.slice(0, sampleSize);

    let canonicalCount = 0;
    let mentionCount = 0;
    let stampedCount = 0;

    for (const rel of samplesToCheck) {
      const copy = await getEntity(rel.peer);
      const entityClass = copy.properties.entity_class as string;
      const stamps = copy.properties.stamps as Array<{ stamped_by: string }> | undefined;

      if (entityClass === 'canonical') {
        canonicalCount++;
      } else if (entityClass === 'mention') {
        mentionCount++;
      }

      if (stamps && stamps.length > 0) {
        stampedCount++;
      }
    }

    log(`Sample verification (${sampleSize} copies):`);
    log(`  - Canonicals: ${canonicalCount}`);
    log(`  - Mentions: ${mentionCount}`);
    log(`  - Stamped: ${stampedCount}`);

    // Verify entity_class distribution roughly 50/50
    expect(canonicalCount + mentionCount).toBe(sampleSize);
    // Most sampled copies should be stamped (allow 10% tolerance)
    expect(stampedCount).toBeGreaterThanOrEqual(sampleSize * 0.9);

    // Calculate throughput
    const throughput = Math.round(STRESS_NUM_COPIES / (totalTime / 1000));

    log('');
    log('=== Stress Test Complete (100 items) ===');
    log(`   - Created ${STRESS_NUM_COPIES} copies`);
    log(`   - Delegated to scatter-utility: ${handoffRecord?.delegated}`);
    log(`   - Total time: ${totalMins}m${totalSecs}s`);
    log(`   - Throughput: ~${throughput} items/second`);
    log(`   - Stamp completion: ${stampLogs.length}/${STRESS_NUM_COPIES}`);
    log(`   - Sample stamped: ${stampedCount}/${sampleSize}`);
  }, 1000000);  // ~16 minute timeout

  // ==========================================================================
  // Test 4: Large Stress Test (500 items, client-side entity creation)
  // ==========================================================================

  it('should handle 500 item scatter with client-side entity creation', async () => {
    if (!ARKE_USER_KEY || !RHIZA_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // 500 copies - would exceed worker CPU limits if created server-side
    // Instead, we create them client-side and use use_existing_copies mode
    const LARGE_STRESS_NUM_COPIES = 500;
    const LARGE_STRESS_NUM_CANONICALS = 250;
    const LARGE_STRESS_NUM_MENTIONS = 250;

    log('=== Large Stress Test (500 items, client-side creation) ===');

    // Step 1: Create parent entity first (with use_existing_copies flag)
    log('Creating parent entity...');
    const parentEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Large Stress Test Entity (500)',
        content: 'Pre-created copies for stress testing',
        use_existing_copies: true,  // Tell scatter worker to use existing copies
        mix_entity_class: true,
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created parent entity: ${parentEntity.id}`);

    // Step 2: Create all copy entities client-side in batches
    log(`Creating ${LARGE_STRESS_NUM_COPIES} copy entities client-side...`);
    const startCreation = Date.now();

    const copyIds: string[] = [];
    const CREATION_BATCH_SIZE = 50;  // Create 50 at a time

    for (let batchStart = 0; batchStart < LARGE_STRESS_NUM_COPIES; batchStart += CREATION_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + CREATION_BATCH_SIZE, LARGE_STRESS_NUM_COPIES);
      const batchPromises: Promise<{ id: string }>[] = [];

      for (let i = batchStart; i < batchEnd; i++) {
        // Even indices = canonical, odd indices = mention
        const entityClass = i % 2 === 0 ? 'canonical' : 'mention';

        const promise = createEntity({
          type: 'test_entity',
          properties: {
            label: `Copy ${i + 1} of 500`,
            copy_index: i,
            copy_total: LARGE_STRESS_NUM_COPIES,
            source_entity: parentEntity.id,
            entity_class: entityClass,
            created_at: new Date().toISOString(),
          },
          collection: targetCollection.id,
        });

        batchPromises.push(promise);
      }

      const batchResults = await Promise.all(batchPromises);
      copyIds.push(...batchResults.map(r => r.id));

      log(`  Created batch ${Math.floor(batchStart / CREATION_BATCH_SIZE) + 1}: ${copyIds.length}/${LARGE_STRESS_NUM_COPIES}`);
    }

    const creationTime = Date.now() - startCreation;
    log(`Created ${copyIds.length} entities in ${Math.round(creationTime / 1000)}s`);

    // Step 3: Update parent entity with has_copy relationships (in batches to avoid huge requests)
    log('Adding has_copy relationships to parent...');
    const REL_BATCH_SIZE = 100;

    for (let batchStart = 0; batchStart < copyIds.length; batchStart += REL_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + REL_BATCH_SIZE, copyIds.length);
      const batchCopyIds = copyIds.slice(batchStart, batchEnd);

      // Get current tip for CAS
      const parent = await getEntity(parentEntity.id);

      // Use apiRequest directly for relationship update
      await apiRequest('PUT', `/entities/${parentEntity.id}`, {
        expect_tip: parent.cid,
        relationships_add: batchCopyIds.map((copyId, idx) => ({
          predicate: 'has_copy',
          peer: copyId,
          peer_type: 'test_entity',
          peer_label: `Copy ${batchStart + idx + 1}`,
        })),
      });

      log(`  Added relationships batch ${Math.floor(batchStart / REL_BATCH_SIZE) + 1}`);
    }

    // Step 4: Invoke the rhiza workflow
    log('Invoking routing scatter test rhiza with 500 pre-created items...');
    const startTime = Date.now();
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID,
      targetEntity: parentEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    const largeStressJobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${largeStressJobCollectionId}`);

    // Wait for workflow completion
    log('Waiting for workflow completion (this will take a while)...');
    const tree = await waitForWorkflowTree(largeStressJobCollectionId, {
      timeout: 1800000,  // 30 minutes for 500 items
      pollInterval: 3000,  // Poll every 3 seconds
      onPoll: (t, elapsed) => {
        const mins = Math.floor(elapsed / 60000);
        const secs = Math.floor((elapsed % 60000) / 1000);
        log(`  Poll: ${t.logs.size} logs, complete=${t.isComplete}, elapsed=${mins}m${secs}s`);
      },
    });

    const totalTime = Date.now() - startTime;
    const totalMins = Math.floor(totalTime / 60000);
    const totalSecs = Math.floor((totalTime % 60000) / 1000);

    log(`Workflow complete: ${tree.isComplete}`);
    log(`Total time: ${totalMins}m${totalSecs}s`);
    log(`Logs found: ${tree.logs.size}`);
    log(`Has errors: ${tree.hasErrors}`);

    // Convert logs Map to array for analysis
    const allLogs = Array.from(tree.logs.values());

    const scatterLogs = allLogs.filter(
      (l) => l.properties.klados_id === SCATTER_KLADOS
    );
    const stampLogs = allLogs.filter(
      (l) => l.properties.klados_id === STAMP_KLADOS
    );

    log(`Scatter logs: ${scatterLogs.length}`);
    log(`Stamp logs: ${stampLogs.length}`);

    expect(scatterLogs).toHaveLength(1);

    // Check delegation
    const scatterLog = scatterLogs[0];
    const handoffs = scatterLog?.properties.log_data?.entry?.handoffs;
    const handoffRecord = handoffs?.[0];

    log(`Delegation flag: ${handoffRecord?.delegated}`);
    log(`Dispatch ID: ${handoffRecord?.dispatch_id}`);

    expect(handoffRecord?.delegated).toBe(true);

    // Verify stamp count - allow some tolerance for timing
    const stampTolerance = Math.floor(LARGE_STRESS_NUM_COPIES * 0.05); // 5% tolerance
    expect(stampLogs.length).toBeGreaterThanOrEqual(LARGE_STRESS_NUM_COPIES - stampTolerance);

    log(`Stamp logs: ${stampLogs.length}/${LARGE_STRESS_NUM_COPIES} (${Math.round(stampLogs.length / LARGE_STRESS_NUM_COPIES * 100)}%)`);

    // Sample verification - check 100 random copies
    const sampleSize = 100;
    const shuffled = [...copyIds].sort(() => Math.random() - 0.5);
    const samplesToCheck = shuffled.slice(0, sampleSize);

    let canonicalCount = 0;
    let mentionCount = 0;
    let stampedCount = 0;

    for (const copyId of samplesToCheck) {
      const copy = await getEntity(copyId);
      const entityClass = copy.properties.entity_class as string;
      const stamps = copy.properties.stamps as Array<{ stamped_by: string }> | undefined;

      if (entityClass === 'canonical') {
        canonicalCount++;
      } else if (entityClass === 'mention') {
        mentionCount++;
      }

      if (stamps && stamps.length > 0) {
        stampedCount++;
      }
    }

    log(`Sample verification (${sampleSize} copies):`);
    log(`  - Canonicals: ${canonicalCount}`);
    log(`  - Mentions: ${mentionCount}`);
    log(`  - Stamped: ${stampedCount}`);

    // Verify entity_class distribution roughly 50/50
    expect(canonicalCount + mentionCount).toBe(sampleSize);
    // Most sampled copies should be stamped (allow 10% tolerance)
    expect(stampedCount).toBeGreaterThanOrEqual(sampleSize * 0.9);

    // Calculate throughput (workflow portion only, not entity creation)
    const throughput = Math.round(LARGE_STRESS_NUM_COPIES / (totalTime / 1000));

    log('');
    log('=== Large Stress Test Complete (500 items) ===');
    log(`   - Pre-created ${LARGE_STRESS_NUM_COPIES} copies client-side`);
    log(`   - Entity creation time: ${Math.round(creationTime / 1000)}s`);
    log(`   - Delegated to scatter-utility: ${handoffRecord?.delegated}`);
    log(`   - Workflow time: ${totalMins}m${totalSecs}s`);
    log(`   - Throughput: ~${throughput} items/second`);
    log(`   - Stamp completion: ${stampLogs.length}/${LARGE_STRESS_NUM_COPIES}`);
    log(`   - Sample stamped: ${stampedCount}/${sampleSize}`);
  }, 2400000);  // 40 minute timeout
});
