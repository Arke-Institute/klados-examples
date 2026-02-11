/**
 * Scatter Worker - Job Processing Logic
 *
 * This worker creates N copies of an entity for scatter testing.
 * Each copy is a new entity with a copy_index property.
 *
 * Used for E2E testing of scatter/gather workflows.
 *
 * Supports `mix_entity_class: true` to assign alternating entity_class
 * values for testing per-item routing in rhiza workflows.
 *
 * Supports `use_existing_copies: true` to use pre-created copies instead
 * of creating new ones (useful for stress testing without hitting worker limits).
 */

import type { KladosJob, Output } from '@arke-institute/rhiza';

/** Default number of copies to create */
const DEFAULT_COPY_COUNT = 3;

/** Batch size for concurrent entity creation/fetching */
const BATCH_SIZE = 20;

/**
 * Process a job by creating N copies of the target entity
 *
 * The number of copies is configurable via the target entity's `copy_count` property.
 * Defaults to 3 if not specified.
 *
 * When `mix_entity_class: true` is set on the target entity, copies will be
 * assigned alternating entity_class values ('canonical' for even indices,
 * 'mention' for odd indices) for testing per-item routing.
 *
 * When `use_existing_copies: true` is set, the worker will read existing
 * `has_copy` relationships instead of creating new entities. This is useful
 * for stress testing with many copies without hitting worker CPU limits.
 *
 * @param job - The KladosJob instance
 * @returns Array of outputs (entity IDs or OutputItems with routing properties)
 */
export async function processJob(job: KladosJob): Promise<Output[]> {
  // Fetch the target entity first to get copy_count
  const target = await job.fetchTarget();

  // Check if we should use existing copies instead of creating new ones
  const useExistingCopies = target.properties.use_existing_copies === true;

  // Read mix_entity_class flag for per-item routing tests
  const mixEntityClass = target.properties.mix_entity_class === true;

  job.log.info('Scatter worker starting', {
    target: job.request.target_entity,
    useExistingCopies,
    mixEntityClass,
    isWorkflow: job.isWorkflow,
  });

  // Use existing copies mode - read from has_copy relationships
  if (useExistingCopies) {
    return await processExistingCopies(job, target, mixEntityClass);
  }

  // Read copy_count from target properties, default to 3
  const numCopies = (target.properties.copy_count as number) || DEFAULT_COPY_COUNT;

  job.log.info('Fetched target entity', {
    id: target.id,
    type: target.type,
    label: target.properties.label,
    requestedCopies: target.properties.copy_count,
    mixEntityClass,
  });

  // Create N copies of the entity in parallel batches
  // Track both ID and entity_class for routing
  const copies: Array<{ id: string; entity_class?: string }> = [];

  for (let batchStart = 0; batchStart < numCopies; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE, numCopies);
    const batchPromises: Promise<{ id: string; entity_class?: string }>[] = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const copyLabel = `${target.properties.label || 'Entity'} - Copy ${i + 1}`;

      // Assign entity_class for routing tests: even = canonical, odd = mention
      const entityClass = mixEntityClass
        ? (i % 2 === 0 ? 'canonical' : 'mention')
        : undefined;

      // Create copy in the target collection with relationship back to original
      const promise = job.client.api.POST('/entities', {
        body: {
          type: target.type,
          collection: job.request.target_collection,
          properties: {
            ...target.properties,
            label: copyLabel,
            copy_index: i,
            copy_total: numCopies,
            source_entity: target.id,
            created_by: job.config.agentId,
            created_at: new Date().toISOString(),
            // Add entity_class for routing tests
            ...(entityClass && { entity_class: entityClass }),
          },
          // Relationship from copy to original
          relationships: [
            {
              predicate: 'copy_of',
              peer: target.id,
              peer_type: target.type,
              peer_label: target.properties.label as string,
            },
          ],
        },
      }).then(({ data, error }) => {
        if (error || !data) {
          throw new Error(`Failed to create copy ${i + 1}: ${JSON.stringify(error)}`);
        }
        return { id: data.id, entity_class: entityClass };
      });

      batchPromises.push(promise);
    }

    // Wait for this batch to complete
    const batchResults = await Promise.all(batchPromises);
    copies.push(...batchResults);

    job.log.info(`Created batch ${Math.floor(batchStart / BATCH_SIZE) + 1}`, {
      batchStart,
      batchEnd,
      totalCreated: copies.length,
    });
  }

  // Update original entity to point to all copies
  // First get the current tip for CAS
  const { data: tipData, error: tipError } = await job.client.api.GET('/entities/{id}/tip', {
    params: { path: { id: target.id } },
  });

  if (tipError || !tipData) {
    job.log.info('Failed to get tip for original entity', { error: tipError });
  } else {
    const { error: updateError } = await job.client.api.PUT('/entities/{id}', {
      params: { path: { id: target.id } },
      body: {
        expect_tip: tipData.cid,
        relationships_add: copies.map((copy, i) => ({
          predicate: 'has_copy',
          peer: copy.id,
          peer_type: target.type,
          peer_label: `${target.properties.label || 'Entity'} - Copy ${i + 1}`,
        })),
      },
    });

    if (updateError) {
      job.log.info('Failed to add has_copy relationships to original', { error: updateError });
      // Don't fail the job - copies were created successfully
    }
  }

  // NOTE: numCopies intentionally removed to test framework's automatic output tracking
  job.log.success('Scatter complete', {
    copyIds: copies.map(c => c.id),
    mixEntityClass,
  });

  // Return outputs - either simple IDs or OutputItems with routing properties
  // When mix_entity_class is true, return objects for per-item routing
  const outputs: Output[] = copies.map(copy => {
    if (copy.entity_class) {
      return {
        entity_id: copy.id,
        entity_class: copy.entity_class,
      };
    }
    return copy.id;
  });

  return outputs;
}

/**
 * Process existing copies instead of creating new ones
 *
 * Reads `has_copy` relationships from the target entity and fetches
 * each copy to get its entity_class property for routing.
 *
 * @param job - The KladosJob instance
 * @param target - The target entity (properties only)
 * @param mixEntityClass - Whether to include entity_class in outputs
 * @returns Array of outputs
 */
async function processExistingCopies(
  job: KladosJob,
  target: { id: string; type: string; properties: Record<string, unknown> },
  mixEntityClass: boolean,
): Promise<Output[]> {
  // Fetch the target entity again to get relationships (fetchTarget doesn't include them)
  const { data: entityWithRels, error: fetchError } = await job.client.api.GET('/entities/{id}', {
    params: { path: { id: target.id } },
  });

  if (fetchError || !entityWithRels) {
    throw new Error(`Failed to fetch target entity with relationships: ${target.id}`);
  }

  // Get has_copy relationships from target
  const copyRelationships = entityWithRels.relationships?.filter(r => r.predicate === 'has_copy') || [];

  if (copyRelationships.length === 0) {
    job.log.info('No existing copies found', { targetId: target.id });
    return [];
  }

  job.log.info('Using existing copies', {
    targetId: target.id,
    copyCount: copyRelationships.length,  // renamed from numCopies to test framework
    mixEntityClass,
  });

  // Fetch each copy entity in batches to get entity_class property
  const copies: Array<{ id: string; entity_class?: string }> = [];

  for (let batchStart = 0; batchStart < copyRelationships.length; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE, copyRelationships.length);
    const batchPromises: Promise<{ id: string; entity_class?: string }>[] = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const rel = copyRelationships[i];
      const promise = job.client.api.GET('/entities/{id}', {
        params: { path: { id: rel.peer } },
      }).then(({ data, error }) => {
        if (error || !data) {
          throw new Error(`Failed to fetch copy ${rel.peer}: ${JSON.stringify(error)}`);
        }
        return {
          id: data.id,
          entity_class: mixEntityClass ? (data.properties.entity_class as string | undefined) : undefined,
        };
      });

      batchPromises.push(promise);
    }

    // Wait for this batch to complete
    const batchResults = await Promise.all(batchPromises);
    copies.push(...batchResults);

    job.log.info(`Fetched batch ${Math.floor(batchStart / BATCH_SIZE) + 1}`, {
      batchStart,
      batchEnd,
      totalFetched: copies.length,
    });
  }

  // NOTE: numCopies intentionally removed to test framework's automatic output tracking
  job.log.success('Scatter complete (using existing copies)', {
    copyIds: copies.map(c => c.id),
    mixEntityClass,
  });

  // Return outputs - either simple IDs or OutputItems with routing properties
  const outputs: Output[] = copies.map(copy => {
    if (copy.entity_class) {
      return {
        entity_id: copy.id,
        entity_class: copy.entity_class,
      };
    }
    return copy.id;
  });

  return outputs;
}
