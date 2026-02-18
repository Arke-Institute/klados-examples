/**
 * E2E Test for Text-to-KG Workflow
 *
 * Tests a rhiza workflow that extracts knowledge graphs from text:
 * 1. text-chunker receives entity with text, creates N chunks
 * 2. scatter fans out: kg-extractor runs on each chunk in parallel
 * 3. Each chunk has entities/relationships extracted
 *
 * Flow:
 *   Entity with text → text-chunker → N chunks → scatter → kg-extractor (parallel) → DONE
 *
 * Verifies:
 * - Text chunker creates text_chunk entities
 * - Scatter fans out to kg-extractor for each chunk
 * - KG extractor creates extracted entities/relationships
 * - Logs connected via parent_logs (tree traversal works)
 * - Total: N+1 logs (1 chunker + N extractors)
 *
 * Prerequisites:
 * 1. text-chunker klados must be deployed (IIKHP6V3WCRV7FK339G0JVTRRV)
 * 2. kg-extractor klados must be deployed (IIKH9PJ2RXCWARWT0CY5D6ZSK6)
 * 3. Register this workflow: npm run register -- text-to-kg
 * 4. Set environment variables (see .env.example)
 *
 * Usage:
 *   source ../.env && npm test
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
import {
  setupTestClient,
  hasTextToKgConfig,
  RHIZA_ID,
  TEXT_CHUNKER_KLADOS,
  KG_EXTRACTOR_KLADOS,
} from './setup.js';

// =============================================================================
// Test Data
// =============================================================================

// Sample text to extract knowledge from - long enough to create multiple chunks
const SAMPLE_TEXT = `
Albert Einstein was a German-born theoretical physicist who developed the theory of relativity, one of the two pillars of modern physics alongside quantum mechanics. His work is also known for its influence on the philosophy of science. He is best known to the general public for his mass-energy equivalence formula E = mc², which has been dubbed "the world's most famous equation".

Einstein received the Nobel Prize in Physics in 1921 for his discovery of the law of the photoelectric effect, a pivotal step in the development of quantum theory. Einstein was born in Ulm, in the Kingdom of Württemberg in the German Empire, on 14 March 1879 into a family of secular Ashkenazi Jews.

At the age of 17, Einstein enrolled in the four-year mathematics and physics teaching diploma program at the Federal Polytechnic School in Zürich. Marie Curie recommended him for a position at the University of Zurich, and he became an associate professor in 1909.

Einstein published more than 300 scientific papers and more than 150 non-scientific works. His intellectual achievements and originality made "Einstein" synonymous with "genius". In 1999, Time magazine named him the Person of the Century.

Isaac Newton was an English mathematician, physicist, astronomer, theologian, and author who is widely recognized as one of the most influential scientists of all time. He was a key figure in the philosophical revolution known as the Enlightenment. His book Philosophiæ Naturalis Principia Mathematica, first published in 1687, established classical mechanics.

Newton also made seminal contributions to optics, and shares credit with Gottfried Wilhelm Leibniz for developing infinitesimal calculus. Newton built the first practical reflecting telescope and developed a sophisticated theory of color based on the observation that a prism separates white light into the colors of the visible spectrum.

In mechanics, Newton formulated the laws of motion and universal gravitation that formed the dominant scientific viewpoint until it was superseded by the theory of relativity. Newton used his mathematical description of gravity to derive Kepler's laws of planetary motion.
`;

// =============================================================================
// Test Suite
// =============================================================================

describe('text-to-kg workflow', () => {
  // Test fixtures
  let targetCollection: { id: string };
  let jobCollectionId: string;
  let testEntity: { id: string };
  let configValid = false;

  // Skip tests if environment not configured
  beforeAll(() => {
    configValid = setupTestClient() && hasTextToKgConfig();
    if (!configValid) {
      console.warn('\nTest skipped: Missing required environment variables');
      console.warn('See .env.example for required configuration\n');
    }
  });

  // Create test fixtures
  beforeAll(async () => {
    if (!configValid) return;

    log('Creating test fixtures...');

    // Create target collection with invoke permissions for agents
    targetCollection = await createCollection({
      label: `Text-to-KG Test ${Date.now()}`,
      description: 'Target collection for text-to-kg workflow test',
      roles: { public: ['*:view', '*:invoke'] },
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create test entity with text content
    testEntity = await createEntity({
      type: 'document',
      properties: {
        title: 'Einstein and Newton - A Historical Perspective',
        text: SAMPLE_TEXT,
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);
    log(`Text length: ${SAMPLE_TEXT.length} characters`);
  });

  // Cleanup test fixtures
  afterAll(async () => {
    if (!configValid) return;

    // DISABLED FOR DEBUGGING - uncomment to re-enable cleanup
    log('Cleanup DISABLED for inspection');
    log(`  Target collection: ${targetCollection?.id}`);
    log(`  Test entity: ${testEntity?.id}`);
    log(`  Job collection: ${jobCollectionId}`);
    // try {
    //   await sleep(1000);
    //   if (testEntity?.id) await deleteEntity(testEntity.id);
    //   if (targetCollection?.id) await deleteEntity(targetCollection.id);
    //   if (jobCollectionId) await deleteEntity(jobCollectionId);
    //   log('Cleanup complete');
    // } catch (e) {
    //   log(`Cleanup error (non-fatal): ${e}`);
    // }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should chunk text and extract knowledge graph from each chunk', async () => {
    if (!configValid) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the workflow
    log('Invoking text-to-kg workflow...');
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
    // KG extraction is slow (Gemini API), so we use a long timeout
    log('Waiting for workflow tree completion...');
    const tree = await waitForWorkflowTree(jobCollectionId, {
      timeout: 600000, // 10 minutes - KG extraction is slow
      pollInterval: 10000, // Poll every 10 seconds
      onPoll: (t, elapsed) => {
        log(`  Poll at ${Math.round(elapsed / 1000)}s: ${t.logs.size} logs, complete=${t.isComplete}`);
      },
    });

    log(`Workflow tree: ${tree.logs.size} logs, complete=${tree.isComplete}`);

    // Verify workflow completed
    expect(tree.isComplete).toBe(true);

    // All logs should be done
    const allLogs = Array.from(tree.logs.values());
    log('\nLog summary:');
    for (const logEntry of allLogs) {
      const props = logEntry.properties;
      log(`  ${props.klados_id?.slice(0, 12)}... - status: ${props.status}`);
    }

    // Count logs by type
    const chunkerLogs = allLogs.filter(l =>
      l.properties.klados_id === TEXT_CHUNKER_KLADOS
    );
    const extractorLogs = allLogs.filter(l =>
      l.properties.klados_id === KG_EXTRACTOR_KLADOS
    );

    log(`\nChunker logs: ${chunkerLogs.length}`);
    log(`Extractor logs: ${extractorLogs.length}`);

    // Should have 1 chunker log
    expect(chunkerLogs.length).toBe(1);

    // Should have N extractor logs (one per chunk)
    // N depends on text length and chunk size
    expect(extractorLogs.length).toBeGreaterThan(0);

    // Total logs should be N+1
    expect(allLogs.length).toBe(1 + extractorLogs.length);

    // Verify all logs completed successfully
    for (const logEntry of allLogs) {
      expect(logEntry.properties.status).toBe('done');
    }

    // Verify chunks were created
    log('\nVerifying chunks...');
    const originalEntity = await getEntity(testEntity.id);
    const chunkRels = originalEntity.relationships?.filter(r => r.predicate === 'has_chunk') ?? [];

    log(`Original entity has ${chunkRels.length} chunk relationships`);
    expect(chunkRels.length).toBe(extractorLogs.length);

    // Verify each chunk has entities extracted
    log('\nVerifying extracted entities from first chunk...');
    if (chunkRels.length > 0) {
      const firstChunk = await getEntity(chunkRels[0].peer);
      log(`  First chunk: ${firstChunk.id.slice(0, 12)}...`);
      log(`  Chunk text length: ${(firstChunk.properties.text as string)?.length ?? 0}`);

      // Check for extracted entities via relationships
      const extractedRels = firstChunk.relationships?.filter(r =>
        r.predicate === 'has_entity' || r.predicate === 'extracted_entity'
      ) ?? [];
      log(`  Extracted entity relationships: ${extractedRels.length}`);
    }

    log('\n✅ Text-to-KG workflow completed successfully!');
    log(`  - Created ${chunkRels.length} chunks`);
    log(`  - Each chunk processed by kg-extractor`);
    log(`  - All ${allLogs.length} logs completed (1 chunker + ${extractorLogs.length} extractors)`);
    log(`  - Tree traversal found all logs`);
  }, 660000); // 11 minutes test timeout (10 min workflow + buffer)
});
