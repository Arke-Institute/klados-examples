/**
 * Shared test setup and utilities for text-to-kg workflow tests
 */

import { configureTestClient } from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

export const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
export const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
export const ARKE_NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';

// Rhiza ID (from registration)
export const RHIZA_ID = process.env.RHIZA_ID;

// Klados IDs for verification
export const TEXT_CHUNKER_KLADOS = process.env.TEXT_CHUNKER_KLADOS;
export const KG_EXTRACTOR_KLADOS = process.env.KG_EXTRACTOR_KLADOS;

// =============================================================================
// Setup
// =============================================================================

/**
 * Initialize the test client
 *
 * Call this in beforeAll() of each test file.
 */
export function setupTestClient(): boolean {
  if (!ARKE_USER_KEY) {
    console.warn('Skipping tests: ARKE_USER_KEY not set');
    return false;
  }

  configureTestClient({
    apiBase: ARKE_API_BASE,
    userKey: ARKE_USER_KEY,
    network: ARKE_NETWORK,
  });

  return true;
}

/**
 * Check if required environment variables are set for text-to-kg test
 */
export function hasTextToKgConfig(): boolean {
  if (!ARKE_USER_KEY) {
    console.warn('Missing: ARKE_USER_KEY');
    return false;
  }
  if (!RHIZA_ID) {
    console.warn('Missing: RHIZA_ID (run: npm run register -- text-to-kg)');
    return false;
  }
  return true;
}
