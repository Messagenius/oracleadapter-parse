// Copyright (c) 2023, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/

/*
  Bounded fan-out for pool-backed work.

  Every adapter operation borrows a connection from the pool, so an unbounded
  `Promise.all` over N items asks for N connections at once. Once demand
  exceeds poolMax the tail of the fan-out sits in the pool queue, and anything
  still waiting after PARSE_ORACLE_QUEUE_TIMEOUT fails with NJS-040. The worst
  case is boot: updateSchemaWithIndexes fans out over every class, and each
  class costs three sequential connections, so a schema with a few dozen
  classes can exhaust the pool before the pod has served its first request.

  mapWithConcurrency keeps the same call shape as `Promise.all(items.map(fn))`
  — input order preserved, first rejection propagates — but never has more
  than `limit` items in flight, and stops starting new ones once one fails.
*/

const DEFAULT_LIMIT = 4;

// Pick a ceiling that leaves the pool room to serve request traffic while the
// fan-out runs. PARSE_ORACLE_MAX_PARALLEL overrides; otherwise stay a
// comfortable margin below poolMax.
export const poolConcurrencyLimit = (adapter, fallback = DEFAULT_LIMIT) => {
  const configured = Number(process.env.PARSE_ORACLE_MAX_PARALLEL);
  if (Number.isFinite(configured) && configured > 0) {
    return Math.floor(configured);
  }
  const pool = adapter && adapter._connectionPool;
  const poolMax = pool && Number(pool.poolMax);
  if (Number.isFinite(poolMax) && poolMax > 1) {
    return Math.max(1, Math.min(fallback, poolMax - 1));
  }
  return fallback;
};

export const mapWithConcurrency = async (items, limit, fn) => {
  const list = Array.from(items || []);
  const results = new Array(list.length);
  if (list.length === 0) {
    return results;
  }

  const ceiling = Math.max(1, Math.min(Math.floor(limit) || 1, list.length));
  let next = 0;
  let failed = false;

  const worker = async () => {
    for (;;) {
      if (failed) {
        return;
      }
      const index = next++;
      if (index >= list.length) {
        return;
      }
      try {
        results[index] = await fn(list[index], index);
      } catch (error) {
        failed = true;
        throw error;
      }
    }
  };

  const workers = [];
  for (let i = 0; i < ceiling; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
};
