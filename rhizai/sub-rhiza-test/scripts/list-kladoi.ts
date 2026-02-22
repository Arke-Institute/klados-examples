/**
 * List available klados workers on the Arke network
 *
 * This helper script lists klados entities you have access to,
 * which can be used in workflow definitions.
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npm run list-kladoi
 */

import { configureTestClient, apiRequest } from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const ARKE_NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';

// =============================================================================
// Types
// =============================================================================

interface KladosEntity {
  id: string;
  type: string;
  properties: {
    label?: string;
    description?: string;
    status?: string;
    endpoint?: string;
  };
}

interface SearchResult {
  entities: Array<{
    id: string;
    type: string;
    label?: string;
  }>;
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }

  console.log(`\nListing klados workers on ${ARKE_NETWORK} network...\n`);

  configureTestClient({
    apiBase: ARKE_API_BASE,
    userKey: ARKE_USER_KEY,
    network: ARKE_NETWORK,
  });

  try {
    // Search for klados entities
    const result = await apiRequest<SearchResult>(
      'GET',
      '/entities?type=klados&limit=50'
    );

    if (!result.entities || result.entities.length === 0) {
      console.log('No klados workers found.');
      console.log('\nTo create a klados worker, use the klados-worker-template:');
      console.log('  git clone https://github.com/Arke-Institute/klados-worker-template');
      return;
    }

    console.log('Available klados workers:\n');
    console.log('ID                              | Label                    | Status');
    console.log('--------------------------------|--------------------------|--------');

    for (const entity of result.entities) {
      // Fetch full entity to get properties
      try {
        const klados = await apiRequest<KladosEntity>('GET', `/entities/${entity.id}`);
        const label = klados.properties.label || '(unnamed)';
        const status = klados.properties.status || 'unknown';
        console.log(
          `${entity.id.padEnd(31)} | ${label.slice(0, 24).padEnd(24)} | ${status}`
        );
      } catch {
        console.log(`${entity.id.padEnd(31)} | (unable to fetch)        |`);
      }
    }

    console.log('\nUse these IDs in your workflow definitions.');
    console.log('Set them as environment variables (e.g., STAMP_KLADOS_1=klados_xxx)');
  } catch (error) {
    console.error('Failed to list kladoi:', error);
    process.exit(1);
  }
}

main();
