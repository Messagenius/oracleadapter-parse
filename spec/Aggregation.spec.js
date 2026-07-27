// Tests for the generic Oracle aggregation engine (Aggregation.js).
//
// These cover the PURE parts (validation + SQL generation + injection
// resistance) and need no database. They run two ways:
//   * under the repo's jasmine harness (npm test), and
//   * standalone for quick iteration:  node spec/Aggregation.spec.js
//
// The standalone shim below only activates when jasmine globals are absent,
// so it never interferes with the real harness.

/* eslint-disable no-console */
if (typeof describe === 'undefined') {
  let passed = 0;
  let failed = 0;
  const stack = [];
  global.describe = (name, fn) => {
    stack.push(name);
    fn();
    stack.pop();
  };
  global.it = (name, fn) => {
    try {
      fn();
      passed++;
      console.log('  ✓ ' + [...stack, name].join(' > '));
    } catch (e) {
      failed++;
      console.log('  ✗ ' + [...stack, name].join(' > ') + '\n      ' + e.message);
    }
  };
  const matchers = actual => ({
    toBe: e => { if (actual !== e) throw new Error(`expected ${JSON.stringify(actual)} to be ${JSON.stringify(e)}`); },
    toEqual: e => { if (JSON.stringify(actual) !== JSON.stringify(e)) throw new Error(`expected ${JSON.stringify(actual)} to equal ${JSON.stringify(e)}`); },
    toContain: e => { if (!String(actual).includes(e)) throw new Error(`expected to contain ${JSON.stringify(e)}\n      in: ${actual}`); },
    toMatch: re => { if (!re.test(String(actual))) throw new Error(`expected to match ${re}\n      in: ${actual}`); },
    toBeGreaterThan: e => { if (!(actual > e)) throw new Error(`expected ${actual} > ${e}`); },
    toThrow: () => { let threw = false; try { actual(); } catch (_) { threw = true; } if (!threw) throw new Error('expected function to throw'); },
    not: {
      toContain: e => { if (String(actual).includes(e)) throw new Error(`expected NOT to contain ${JSON.stringify(e)}\n      in: ${actual}`); },
    },
  });
  global.expect = matchers;
  process.on('exit', () => {
    console.log(`\nAggregation.spec: ${passed} passed, ${failed} failed`);
    if (failed > 0) process.exitCode = 1;
  });
}

const Agg = require('../Aggregation');
const { buildAggregationSql, clampLimit, toOracleTimestampText, MAX_LIMIT } = Agg;

// Shared fixtures ----------------------------------------------------------
const tables = {
  Message: { tableName: 'Message', contentColumn: 'JSON_DOCUMENT' },
  Conversation: { tableName: 'Conversation', contentColumn: 'JSON_DOCUMENT' },
  MessageReport: { tableName: 'MessageReport', contentColumn: 'JSON_DOCUMENT' },
  Friendship: { tableName: 'Friendship', contentColumn: 'JSON_DOCUMENT' },
};
const msgSchema = {
  fields: {
    from: { type: 'Pointer', targetClass: '_User' },
    conversation: { type: 'Pointer', targetClass: 'Conversation' },
    score: { type: 'Number' },
  },
};
const convSchema = { fields: { type: { type: 'Number' }, isChannel: { type: 'Boolean' } } };
const reportSchema = {
  fields: {
    reporter: { type: 'Pointer', targetClass: '_User' },
    reportedUser: { type: 'Pointer', targetClass: '_User' },
  },
};
const friendSchema = { fields: { from: { type: 'Pointer', targetClass: '_User' }, status: { type: 'Number' } } };

const build = spec => buildAggregationSql(spec, tables);

describe('Aggregation: validation & injection resistance', () => {
  it('rejects an unknown field', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, measures: [{ as: 'c', op: 'count' }], groupBy: [{ as: 'x', field: 'nope' }] })).toThrow();
  });
  it('rejects an unsupported measure op', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, measures: [{ as: 'c', op: 'median', field: 'score' }] })).toThrow();
  });
  it('rejects an injection attempt in className', () => {
    expect(() => build({ className: 'Message"; DROP TABLE x--', schema: msgSchema, measures: [{ as: 'c', op: 'count' }] })).toThrow();
  });
  it('rejects an injection attempt in a field name', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, measures: [{ as: 'c', op: 'count' }], groupBy: [{ as: 'g', field: "x') OR 1=1--" }] })).toThrow();
  });
  it('requires at least one measure', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, groupBy: [{ as: 'g', field: 'score' }] })).toThrow();
  });
  it('rejects orderBy on a non-selected alias', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, measures: [{ as: 'c', op: 'count' }], orderBy: [{ key: 'ghost', direction: 'desc' }] })).toThrow();
  });
  it('clamps limit to MAX_LIMIT', () => {
    expect(clampLimit(10 ** 9)).toBe(MAX_LIMIT);
    expect(clampLimit(-5)).toBeGreaterThan(0);
    expect(clampLimit(250)).toBe(250);
  });
});

describe('Aggregation: date grouping (UTC)', () => {
  it('dateTrunc day -> TRUNC + TO_CHAR over RETURNING TIMESTAMP', () => {
    const { sql } = build({
      className: 'Message', schema: msgSchema,
      groupBy: [{ as: 'day', op: 'dateTrunc', field: 'createdAt', unit: 'day' }],
      measures: [{ as: 'count', op: 'count' }],
    });
    expect(sql).toContain("TO_CHAR(TRUNC(JSON_VALUE(t0.\"JSON_DOCUMENT\", '$.createdAt' RETURNING TIMESTAMP)), 'YYYY-MM-DD')");
    expect(sql).toContain('AS "day"');
    expect(sql).toContain('COUNT(*) AS "count"');
  });
  it('datePart hour -> EXTRACT(HOUR ...)', () => {
    const { sql } = build({
      className: 'Message', schema: msgSchema,
      groupBy: [{ as: 'hour', op: 'datePart', field: 'createdAt', part: 'hour' }],
      measures: [{ as: 'count', op: 'count' }],
    });
    expect(sql).toContain("EXTRACT(HOUR FROM JSON_VALUE(t0.\"JSON_DOCUMENT\", '$.createdAt' RETURNING TIMESTAMP))");
  });
});

describe('Aggregation: measures', () => {
  it('count + countDistinct(pointer) extracts the objectId', () => {
    const { sql } = build({
      className: 'MessageReport', schema: reportSchema,
      groupBy: [{ as: 'uid', field: 'reportedUser' }],
      measures: [{ as: 'count', op: 'count' }, { as: 'distinctReporters', op: 'countDistinct', field: 'reporter' }],
      orderBy: [{ key: 'count', direction: 'desc' }], limit: 100,
    });
    // pointer grouping extracts objectId after '$'
    expect(sql).toContain("SUBSTR(JSON_VALUE(t0.\"JSON_DOCUMENT\", '$._p_reportedUser')");
    expect(sql).toContain("COUNT(DISTINCT SUBSTR(JSON_VALUE(t0.\"JSON_DOCUMENT\", '$._p_reporter')");
    expect(sql).toContain('ORDER BY "count" DESC');
  });
  it('avg executes in Oracle with RETURNING NUMBER', () => {
    const { sql } = build({
      className: 'Message', schema: msgSchema,
      measures: [{ as: 'averageValue', op: 'avg', field: 'score' }],
    });
    expect(sql).toContain("AVG(JSON_VALUE(t0.\"JSON_DOCUMENT\", '$.score' RETURNING NUMBER)) AS \"averageValue\"");
  });
  it('avg rejects a non-numeric field', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, measures: [{ as: 'a', op: 'avg', field: 'from' }] })).toThrow();
  });
  it('conditional bucket compiles to COUNT(CASE ...) with a bound value', () => {
    const { sql, binds } = build({
      className: 'Friendship', schema: friendSchema,
      groupBy: [{ as: 'uid', field: 'from' }],
      measures: [
        { as: 'total', op: 'count' },
        { as: 'accepted', op: 'count', where: { field: 'status', eq: 2 } },
      ],
    });
    expect(sql).toContain('COUNT(CASE WHEN');
    expect(sql).toContain("JSON_VALUE(t0.\"JSON_DOCUMENT\", '$.status' RETURNING NUMBER) =");
    expect(Object.values(binds)).toContain(2); // the literal 2 is a bind, not inline
  });
});

describe('Aggregation: filters use bind variables', () => {
  it('date range is bound, never inlined', () => {
    const start = new Date('2026-06-01T00:00:00.000Z');
    const { sql, binds } = build({
      className: 'Message', schema: msgSchema,
      filter: { createdAt: { $gte: start } },
      measures: [{ as: 'count', op: 'count' }],
    });
    expect(sql).toContain('TO_TIMESTAMP(:b');
    expect(sql).not.toContain('2026-06-01'); // value only lives in binds
    expect(Object.values(binds)).toContain('2026-06-01T00:00:00.000');
  });
  it('$in produces a bound IN-list', () => {
    const { sql, binds } = build({
      className: 'MessageReport', schema: reportSchema,
      filter: { reportedUser: { $in: ['a1', 'b2', 'c3'] } },
      measures: [{ as: 'count', op: 'count' }],
    });
    expect(sql).toContain('IN (:b');
    expect(Object.values(binds)).toContain('a1');
    expect(Object.values(binds)).toContain('c3');
  });
});

describe('Aggregation: generic relationship lookup (pointer join)', () => {
  it('joins Message -> Conversation and groups by a foreign field', () => {
    const { sql } = build({
      className: 'Message', schema: msgSchema,
      lookups: [{ as: 'conv', localField: 'conversation', foreignClass: 'Conversation', foreignSchema: convSchema }],
      groupBy: [
        { as: 'day', op: 'dateTrunc', field: 'createdAt', unit: 'day' },
        { as: 'convType', field: 'conv.type' },
      ],
      measures: [{ as: 'count', op: 'count' }],
    });
    expect(sql).toContain('LEFT JOIN "Conversation" t1 ON');
    expect(sql).toContain("JSON_VALUE(t1.\"JSON_DOCUMENT\", '$._id') = SUBSTR(JSON_VALUE(t0.\"JSON_DOCUMENT\", '$._p_conversation')");
    expect(sql).toContain("JSON_VALUE(t1.\"JSON_DOCUMENT\", '$.type' RETURNING NUMBER) AS \"convType\"");
  });
  it('rejects a lookup whose localField is not a Pointer', () => {
    expect(() => build({
      className: 'Message', schema: msgSchema,
      lookups: [{ as: 'rel', localField: 'score', foreignClass: 'Conversation', foreignSchema: convSchema }],
      measures: [{ as: 'count', op: 'count' }],
    })).toThrow();
  });
});

describe('Aggregation: limit is always applied & bound', () => {
  it('appends FETCH FIRST :bind ROWS ONLY', () => {
    const { sql, binds } = build({ className: 'Message', schema: msgSchema, measures: [{ as: 'count', op: 'count' }], limit: 42 });
    expect(sql).toContain('FETCH FIRST :b');
    expect(Object.values(binds)).toContain(42);
  });
});

describe('Aggregation: compound and/or buckets', () => {
  it('compiles { and:[...] } to a parenthesized AND with bound values', () => {
    const { sql, binds } = build({
      className: 'Message', schema: msgSchema,
      lookups: [{ as: 'conv', localField: 'conversation', foreignClass: 'Conversation', foreignSchema: convSchema }],
      measures: [{ as: 'channels', op: 'count', where: { and: [{ field: 'conv.type', eq: 1 }, { field: 'conv.isChannel', eq: true }] } }],
    });
    expect(sql).toMatch(/COUNT\(CASE WHEN \(.*AND.*\) THEN 1 END\)/);
    expect(Object.values(binds)).toContain(1);
    expect(Object.values(binds)).toContain('true'); // boolean compared as JSON text
  });
});

describe('Aggregation: generic array-length aggregate', () => {
  it('sumArrayLength -> SUM(NVL(JSON_VALUE(... $.field.size() ...),0))', () => {
    const { sql } = build({
      className: 'Conversation', schema: { fields: { participantsIds: { type: 'Array' } } },
      measures: [{ as: 'parts', op: 'sumArrayLength', field: 'participantsIds' }],
    });
    expect(sql).toContain("SUM(NVL(JSON_VALUE(t0.\"JSON_DOCUMENT\", '$.participantsIds.size()' RETURNING NUMBER), 0)) AS \"parts\"");
  });
  it('rejects array-length on a Pointer field', () => {
    expect(() => build({ className: 'Message', schema: msgSchema, measures: [{ as: 'x', op: 'sumArrayLength', field: 'from' }] })).toThrow();
  });
});

describe('Aggregation: toOracleTimestampText (UTC canonicalization)', () => {
  it('formats a Date to YYYY-MM-DDTHH:MM:SS.mmm without Z', () => {
    expect(toOracleTimestampText(new Date('2026-06-20T12:00:28.101Z'))).toBe('2026-06-20T12:00:28.101');
  });
});
