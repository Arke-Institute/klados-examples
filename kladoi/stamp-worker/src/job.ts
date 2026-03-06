/**
 * Stamp Worker - Job Processing Logic
 *
 * This worker stamps entities with metadata to prove it processed them.
 * Stamps accumulate in an array, allowing multiple workflow steps to
 * add their own stamps without overwriting previous ones.
 *
 * Used for E2E testing of the klados worker template and workflow chaining.
 */

import type { KladosJob } from '@arke-institute/rhiza';

/**
 * Process a job by stamping the target entity
 *
 * @param job - The KladosJob instance
 * @returns Array of output entity IDs (the stamped entity)
 */
export async function processJob(job: KladosJob): Promise<string[]> {
  job.log.info('Stamp worker starting', {
    target: job.request.target_entity,
    isWorkflow: job.isWorkflow,
  });

  // Fetch the target entity
  const target = await job.fetchTarget();
  job.log.info('Fetched target entity', {
    id: target.id,
    type: target.type,
  });

  // Get current tip for CAS update
  const { data: tipData, error: tipError } = await job.client.api.GET('/entities/{id}/tip', {
    params: { path: { id: target.id } },
  });

  if (tipError || !tipData) {
    throw new Error(`Failed to get entity tip: ${target.id}`);
  }

  // Get existing stamps array or initialize empty
  const existingStamps = Array.isArray(target.properties.stamps)
    ? target.properties.stamps
    : [];

  // Create new stamp entry
  const newStamp = {
    stamped_at: new Date().toISOString(),
    stamped_by: job.config.agentId,
    stamp_number: existingStamps.length + 1,
    stamp_message: `Stamp #${existingStamps.length + 1} - This worker was here!`,
    job_id: job.request.job_id,
  };

  // Accumulate stamps
  const stamps = [...existingStamps, newStamp];

  const { error: updateError } = await job.client.api.PUT('/entities/{id}', {
    params: { path: { id: target.id } },
    body: {
      expect_tip: tipData.cid,
      properties: {
        ...target.properties,
        stamps,
        stamp_count: stamps.length,
      },
    },
  });

  if (updateError) {
    throw new Error(`Failed to stamp entity: ${JSON.stringify(updateError)}`);
  }

  job.log.info('Entity stamped successfully', {
    stamp_number: newStamp.stamp_number,
    total_stamps: stamps.length,
    stamped_by: newStamp.stamped_by,
  });

  // Create a receipt entity to prove we processed this
  const { data: receipt, error: receiptError } = await job.client.api.POST('/entities', {
    body: {
      type: 'stamp_receipt',
      collection: job.request.target_collection,
      properties: {
        stamped_entity: target.id,
        stamp_number: newStamp.stamp_number,
        stamped_at: newStamp.stamped_at,
        stamped_by: newStamp.stamped_by,
        job_id: job.request.job_id,
      },
    },
  });

  if (receiptError || !receipt) {
    throw new Error(`Failed to create receipt: ${JSON.stringify(receiptError)}`);
  }

  job.log.info('Created stamp receipt', { receipt_id: receipt.id });
  job.log.success('Job completed');

  // Return both the updated entity and the new receipt as outputs
  return [target.id, receipt.id];
}
