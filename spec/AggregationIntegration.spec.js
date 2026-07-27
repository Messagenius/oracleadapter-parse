// Live integration test for the generic Oracle aggregation engine + the
// Messagenius metric handlers, run against a REAL local Oracle.
//
// It seeds a small, deterministic dataset (correctness), then 30k rows
// (bounded-result proof), asserts results, and cleans up after itself.
//
// Run (inside the api-server container, against lib/):
//   node lib/Adapters/Storage/Oracle/spec/AggregationIntegration.spec.js
//
// Requires env MSG_API_DB_URL + ORACLE_CLIENT_LOCATION (present in the container).

/* eslint-disable no-console */
'use strict';

const oracledb = require('oracledb');
const AdapterMod = require('../OracleStorageAdapter');
const OracleStorageAdapter = AdapterMod.default || AdapterMod;
// oracleMetrics lives under api-server/node_modules (different tree than parse-server).
let om;
try { om = require('@messagenius/messagenius-base-api/lib/metrics/oracleMetrics'); }
catch (e) { om = require('/opt/msg/api-server/node_modules/@messagenius/messagenius-base-api/lib/metrics/oracleMetrics'); }

let pass = 0, fail = 0;
const eq = (actual, expected, msg) => {
  const a = JSON.stringify(actual), e = JSON.stringify(expected);
  if (a === e) { pass++; console.log('  ✓ ' + msg); }
  else { fail++; console.log('  ✗ ' + msg + `\n      expected: ${e}\n      actual:   ${a}`); }
};
const ok = (cond, msg) => { if (cond) { pass++; console.log('  ✓ ' + msg); } else { fail++; console.log('  ✗ ' + msg); } };

const TS = '.000000';
const at = (d, h) => `2026-06-${d}T${String(h).padStart(2, '0')}:30:00${TS}`; // UTC, no Z
const ptr = (cls, id) => `${cls}$${id}`;

// --- deterministic seed -----------------------------------------------------
const conversations = [
  { _id: 'aggtest_convG', type: 1, createdAt: at('18', 0) }, // group
  { _id: 'aggtest_convD', type: 0, createdAt: at('18', 0) }, // 1:1
];
const messages = [
  { _id: 'aggtest_m1', createdAt: at('18', 3), _p_from: ptr('_User', 'aggtest_uA'), _p_conversation: ptr('Conversation', 'aggtest_convG') },
  { _id: 'aggtest_m2', createdAt: at('18', 10), _p_from: ptr('_User', 'aggtest_uA'), _p_conversation: ptr('Conversation', 'aggtest_convG') },
  { _id: 'aggtest_m3', createdAt: at('18', 10), _p_from: ptr('_User', 'aggtest_uB'), _p_conversation: ptr('Conversation', 'aggtest_convD') },
  { _id: 'aggtest_m4', createdAt: at('18', 15), _p_from: ptr('_User', 'aggtest_uA'), _p_conversation: ptr('Conversation', 'aggtest_convD') },
  { _id: 'aggtest_m5', createdAt: at('18', 22), _p_from: ptr('_User', 'aggtest_uA'), _p_conversation: ptr('Conversation', 'aggtest_convG') },
  { _id: 'aggtest_m6', createdAt: at('19', 9), _p_from: ptr('_User', 'aggtest_uB'), _p_conversation: ptr('Conversation', 'aggtest_convG') },
];
const reports = [
  { _id: 'aggtest_r1', createdAt: at('18', 1), _p_reporter: ptr('_User', 'aggtest_uA'), _p_reportedUser: ptr('_User', 'aggtest_uB') },
  { _id: 'aggtest_r2', createdAt: at('18', 2), _p_reporter: ptr('_User', 'aggtest_uA'), _p_reportedUser: ptr('_User', 'aggtest_uB') },
  { _id: 'aggtest_r3', createdAt: at('18', 3), _p_reporter: ptr('_User', 'aggtest_uB'), _p_reportedUser: ptr('_User', 'aggtest_uA') },
];
const friendships = [
  { _id: 'aggtest_f1', createdAt: at('18', 1), _p_from: ptr('_User', 'aggtest_uA'), status: 2 },
  { _id: 'aggtest_f2', createdAt: at('18', 2), _p_from: ptr('_User', 'aggtest_uA'), status: 1 },
  { _id: 'aggtest_f3', createdAt: at('18', 3), _p_from: ptr('_User', 'aggtest_uA'), status: 0 },
  { _id: 'aggtest_f4', createdAt: at('18', 4), _p_from: ptr('_User', 'aggtest_uB'), status: 2 },
];
const calls = [
  { _id: 'aggtest_c1', createdAt: at('18', 10), callerId: 'cidA', callerFirstName: 'Alice', callerLastName: 'A', _p_caller: ptr('_User', 'aggtest_uA') },
  { _id: 'aggtest_c2', createdAt: at('18', 14), callerId: 'cidA', callerFirstName: 'Alice', callerLastName: 'A', _p_caller: ptr('_User', 'aggtest_uA') },
  { _id: 'aggtest_c3', createdAt: at('18', 10), callerId: 'cidB', callerFirstName: 'Bob', callerLastName: 'B', _p_caller: ptr('_User', 'aggtest_uB') },
];

// Mock Parse for the name-resolution path (bounded follow-up query).
const mockParse = {
  User: '_User',
  Query: class {
    constructor() { this._in = []; }
    containedIn(_f, arr) { this._in = arr; return this; }
    select() { return this; }
    limit() { return this; }
    async find() { return this._in.map(id => ({ id, get: k => ({ firstName: 'F_' + id, lastName: 'L_' + id }[k] || null) })); }
  },
};

async function insert(conn, collName, docs) {
  const soda = conn.getSodaDatabase();
  const coll = await soda.openCollection(collName);
  if (!coll) throw new Error('missing collection: ' + collName);
  await coll.insertMany(docs);
}
async function cleanup(conn) {
  for (const c of ['Message', 'Conversation', 'MessageReport', 'Friendship', 'RTCCall']) {
    await conn.execute(`DELETE FROM "${c}" WHERE JSON_VALUE("JSON_DOCUMENT", '$._id') LIKE :p`, { p: 'aggtest_%' });
    await conn.execute(`DELETE FROM "${c}" WHERE JSON_VALUE("JSON_DOCUMENT", '$._id') LIKE :p`, { p: 'bulk_%' });
  }
  await conn.commit();
}

async function main() {
  const adapter = new OracleStorageAdapter({ databaseURI: process.env.MSG_API_DB_URL, collectionPrefix: '' });
  await adapter.connect();
  const pool = oracledb.getPool('parse');
  let conn = await pool.getConnection();
  const range = new Date('2026-06-01T00:00:00.000Z'); // covers the seed
  const ctx = { adapter, Parse: mockParse, rangeStartChart: range, chartDays: 90 };

  try {
    console.log('Seeding deterministic dataset...');
    await cleanup(conn); // start clean
    await insert(conn, 'Conversation', conversations);
    await insert(conn, 'Message', messages);
    await insert(conn, 'MessageReport', reports);
    await insert(conn, 'Friendship', friendships);
    await insert(conn, 'RTCCall', calls);
    await conn.commit();

    console.log('\nCorrectness assertions (Oracle DB-side):');

    // (Filter to our seeded days; the DB may already hold unrelated messages.)
    const mine = rows => rows.filter(r => /2026-06-1[89]/.test(r.message_date));

    // Messages Per Day (pointer LOOKUP to Conversation for group vs 1:1)
    eq(mine(await om.HANDLERS['Messages Per Day'](ctx)), [
      { message_date: '2026-06-18T00:00:00.000Z', messages: '5', '1:1': '2', Groups: '3' },
      { message_date: '2026-06-19T00:00:00.000Z', messages: '1', '1:1': '0', Groups: '1' },
    ], 'Messages Per Day (group/1:1 via Conversation lookup)');

    // Messages Per Hour Per Day (day x hour grouping -> 14 buckets)
    const mph = mine(await om.HANDLERS['Messages Per Hour Per Day'](ctx));
    ok(mph.length === 2, 'Messages Per Hour Per Day: one row per seeded day');
    eq([mph[0]['00:00 - 08:59'], mph[0]['10:00 - 10:59'], mph[0]['15:00 - 15:59'], mph[0]['21:00 - 23:59']],
      ['1', '2', '1', '1'], 'Messages Per Hour Per Day: 2026-06-18 hour buckets');
    eq(mph[1]['09:00 - 09:59'], '1', 'Messages Per Hour Per Day: 2026-06-19 hour 09 bucket');

    // Calls Per Day + Calls Per Hour Per Day
    eq(await om.HANDLERS['Calls Per Day'](ctx), [{ call_date: '2026-06-18T00:00:00.000Z', count: '3' }], 'Calls Per Day');
    const cph = await om.HANDLERS['Calls Per Hour Per Day'](ctx);
    eq([cph[0]['10:00 - 10:59'], cph[0]['14:00 - 14:59']], ['2', '1'], 'Calls Per Hour Per Day: hour buckets');

    // Top Reported Users (group-by pointer + countDistinct + name resolution)
    eq(await om.HANDLERS['Top Reported Users'](ctx), [
      { _userId: 'aggtest_uB', User: 'F_aggtest_uB L_aggtest_uB', 'Times Reported': '2', 'Reported By (unique users)': '1' },
      { _userId: 'aggtest_uA', User: 'F_aggtest_uA L_aggtest_uA', 'Times Reported': '1', 'Reported By (unique users)': '1' },
    ], 'Top Reported Users (countDistinct reporters + names)');

    // Top Reporters
    eq(await om.HANDLERS['Top Reporters'](ctx), [
      { _userId: 'aggtest_uA', Reporter: 'F_aggtest_uA L_aggtest_uA', 'Reports Submitted': '2', 'Distinct Users Reported': '1' },
      { _userId: 'aggtest_uB', Reporter: 'F_aggtest_uB L_aggtest_uB', 'Reports Submitted': '1', 'Distinct Users Reported': '1' },
    ], 'Top Reporters');

    // Friendship Requests by User (conditional status buckets)
    eq(await om.HANDLERS['Friendship Requests by User'](ctx), [
      { _userId: 'aggtest_uA', username: 'F_aggtest_uA L_aggtest_uA', Accepted: '1', Pending: '1', Denied: '1' },
      { _userId: 'aggtest_uB', username: 'F_aggtest_uB L_aggtest_uB', Accepted: '1', Pending: '0', Denied: '0' },
    ], 'Friendship by User (status buckets)');

    // Top Calls by Caller (group-by string + max denormalized fields)
    const tcc = await om.HANDLERS['Top Calls by Caller'](ctx);
    eq(tcc.map(r => [r.callerId, r.call_count, r._userId]), [['Alice A', '2', 'aggtest_uA'], ['Bob B', '1', 'aggtest_uB']],
      'Top Calls by Caller (count + denormalized display + caller objectId)');
    ok(tcc[0].latest_call === '2026-06-18T14:30:00.000Z', 'Top Calls by Caller: latest_call = MAX(createdAt) in Oracle');

    // Top Message Senders by Hour (top-N then per-hour, two bounded queries)
    const tms = await om.HANDLERS['Top Message Senders by Hour'](ctx);
    eq(tms.map(r => [r._senderId, r.hour_of_day, r.messages_in_hour, r.total_messages]),
      [['aggtest_uA', 3, '1', '4'], ['aggtest_uA', 10, '1', '4'], ['aggtest_uA', 15, '1', '4'], ['aggtest_uA', 22, '1', '4'],
       ['aggtest_uB', 9, '1', '2'], ['aggtest_uB', 10, '1', '2']],
      'Top Message Senders by Hour (top-N + per-hour, UTC)');

    // Top Callers by Hour
    const tch = await om.HANDLERS['Top Callers by Hour'](ctx);
    eq(tch.map(r => [r.callerId, r.hour_of_day, r.calls_in_hour, r.total_calls]),
      [['Alice A', 10, '1', '2'], ['Alice A', 14, '1', '2'], ['Bob B', 10, '1', '1']],
      'Top Callers by Hour');

    // ---- BOUNDED-RESULT PROOF: 30,000 rows -> tiny aggregate -------------
    console.log('\nBounded-result proof (30,000 messages):');
    const N = 30000, DAYS = 5;
    const bulk = [];
    for (let i = 0; i < N; i++) {
      const day = 1 + (i % DAYS); // 2026-05-01 .. 2026-05-05
      const hour = i % 24;
      bulk.push({
        _id: 'bulk_' + i,
        createdAt: `2026-05-0${day}T${String(hour).padStart(2, '0')}:15:00.000000`,
        _p_from: ptr('_User', 'aggtest_uA'),
        _p_conversation: ptr('Conversation', i % 2 ? 'aggtest_convG' : 'aggtest_convD'),
      });
    }
    const tIns = Date.now();
    const soda = conn.getSodaDatabase();
    const mcoll = await soda.openCollection('Message');
    for (let i = 0; i < bulk.length; i += 2000) { await mcoll.insertMany(bulk.slice(i, i + 2000)); }
    await conn.commit();
    console.log(`  inserted ${N} messages in ${Date.now() - tIns}ms`);

    const t0 = Date.now();
    const rows = await adapter.runAggregation({
      className: 'Message', schema: { fields: { from: { type: 'Pointer' }, conversation: { type: 'Pointer' } } },
      filter: { createdAt: { $gte: new Date('2026-05-01T00:00:00Z'), $lt: new Date('2026-05-06T00:00:00Z') } },
      groupBy: [{ as: 'day', op: 'dateTrunc', field: 'createdAt', unit: 'day' }],
      measures: [{ as: 'messages', op: 'count' }],
      orderBy: [{ key: 'day', direction: 'asc' }], limit: 400,
    });
    const ms = Date.now() - t0;
    const totalCounted = rows.reduce((s, r) => s + Number(r.messages), 0);
    console.log(`  aggregation returned ${rows.length} rows in ${ms}ms; sum(messages)=${totalCounted}`);
    ok(rows.length === DAYS, `30k rows reduced to ${DAYS} aggregate rows (got ${rows.length}) — NOT materialized in Node`);
    ok(totalCounted === N, `aggregate sum equals ${N} (computed in Oracle)`);
    ok(rows.length <= 400, 'result is bounded by aggregation cardinality, not row count');

  } finally {
    console.log('\nCleaning up seeded data...');
    try { await cleanup(conn); } catch (e) { console.log('  cleanup warning:', e.message); }
    await conn.close();
    try { await oracledb.getPool('parse').close(2); } catch (e) {}
  }

  console.log(`\nAggregationIntegration: ${pass} passed, ${fail} failed`);
  process.exitCode = fail > 0 ? 1 : 0;
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
