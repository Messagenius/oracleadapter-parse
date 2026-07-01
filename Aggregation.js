// Copyright (c) 2024, Messagenius / Oracle adapter contributors.
// Licensed under the Universal Permissive License v 1.0.
//
// Generic, backend-neutral aggregation engine for the Oracle (SODA/JSON) Parse
// adapter.
//
// Goals & guarantees
// ------------------
//  * Reusable by ANY Oracle-adapter consumer. Contains NO domain-specific names
//    (no Message / RTCCall / dashboard concepts).
//  * Runs aggregation INSIDE Oracle and returns only small aggregate rows; raw
//    documents are never materialized in Node.
//  * Not an arbitrary-SQL gateway. The caller submits a validated specification;
//    every operation, field and identifier is checked against an allow-list and
//    the Parse schema. Every value, date, range and limit is passed as a bind
//    variable. Identifiers (table/column names) come from SODA metadata or are
//    derived from schema-validated field names — untrusted text never reaches SQL.
//
// Specification shape (all parts optional except className + at least one measure)
// ------------------------------------------------------------------------------
//   {
//     className: 'SomeClass',
//     schema:    { fields: { ... } },          // Parse schema (for field validation)
//     filter:    { field: value | { $gte, $gt, $lte, $lt, $ne, $in, $exists } },
//     lookups:   [ { as: 'rel', localField: 'ptrField',
//                    foreignClass: 'OtherClass', foreignSchema: {fields} } ],
//     groupBy:   [ { as, field } |
//                  { as, op:'dateTrunc', field, unit:'day' } |
//                  { as, op:'datePart',  field, part:'hour' } ],
//     measures:  [ { as, op:'count' } |
//                  { as, op:'countDistinct'|'sum'|'avg'|'min'|'max', field } |
//                  { as, op:'count', where:{ field, <cmp>, value } } ],   // bucket
//     orderBy:   [ { key, direction:'asc'|'desc' } ],   // key = group/measure alias
//     limit:     <int, clamped to MAX_LIMIT>
//   }
//
// A field may be "rel.foreignField" to reference a looked-up class.

const MAX_LIMIT = 10000;
const DEFAULT_LIMIT = 1000;

const SAFE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;     // class names, field keys, aliases
const SAFE_REL = /^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$/; // rel.field

const MEASURE_OPS = new Set(['count', 'countDistinct', 'sum', 'avg', 'min', 'max', 'sumArrayLength', 'avgArrayLength', 'maxArrayLength']);
const NUMERIC_MEASURE_OPS = new Set(['sum', 'avg']); // require a Number field, return number
// Generic JSON array-length aggregates (e.g. SUM of an array field's element count),
// computed DB-side via Oracle's '$.field.size()' item method. Domain-neutral.
const ARRAY_LENGTH_OPS = { sumArrayLength: 'SUM', avgArrayLength: 'AVG', maxArrayLength: 'MAX' };
const COMPARATORS = new Set(['eq', 'ne', 'lt', 'lte', 'gt', 'gte', 'in']);
const FILTER_OPS = { $gte: 'gte', $gt: 'gt', $lte: 'lte', $lt: 'lt', $ne: 'ne', $eq: 'eq', $in: 'in', $exists: 'exists' };
const DATE_UNITS = new Set(['day']);
const DATE_PARTS = new Set(['hour']);

// Built-in Parse field -> stored JSON key (mirrors OracleTransform.transformKey).
const BUILTIN_KEYS = { objectId: '_id', createdAt: 'createdAt', updatedAt: 'updatedAt' };
const BUILTIN_TYPES = { objectId: 'String', createdAt: 'Date', updatedAt: 'Date', _id: 'String' };

class AggregationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AggregationError';
  }
}

const fail = msg => {
  throw new AggregationError(msg);
};

// ---------------------------------------------------------------------------
// Schema / field resolution
// ---------------------------------------------------------------------------

// Resolve a (possibly "rel.field") reference to:
//   { sourceAlias, jsonKey, parseType, isPointer }
// `tables` maps a source-alias ('t0' for base, or a lookup `as`) to its schema.
function resolveField(ref, ctx) {
  if (typeof ref !== 'string' || !ref.length) fail('field reference must be a non-empty string');

  let alias = ctx.baseAlias;
  let fieldName = ref;
  let schema = ctx.baseSchema;

  if (ref.includes('.')) {
    if (!SAFE_REL.test(ref)) fail(`invalid relationship field reference: ${ref}`);
    const [relName, foreign] = ref.split('.');
    const lookup = ctx.lookupsByAs[relName];
    if (!lookup) fail(`unknown lookup alias in field reference: ${relName}`);
    alias = lookup.sqlAlias;
    fieldName = foreign;
    schema = lookup.foreignSchema;
  } else if (!SAFE_IDENT.test(ref)) {
    fail(`invalid field name: ${ref}`);
  }

  // Determine type + storage key.
  let parseType;
  let isPointer = false;
  let jsonKey;

  if (Object.prototype.hasOwnProperty.call(BUILTIN_KEYS, fieldName)) {
    jsonKey = BUILTIN_KEYS[fieldName];
    parseType = BUILTIN_TYPES[fieldName];
  } else {
    const fieldDef = schema && schema.fields ? schema.fields[fieldName] : undefined;
    if (!fieldDef) fail(`field not found in schema: ${fieldName}`);
    parseType = fieldDef.type;
    if (fieldDef.type === 'Pointer') {
      isPointer = true;
      jsonKey = '_p_' + fieldName;
    } else {
      jsonKey = fieldName;
    }
  }

  if (!SAFE_IDENT.test(jsonKey.replace(/^_p_/, ''))) fail(`unsafe storage key derived from field: ${fieldName}`);
  return { sqlAlias: alias, jsonKey, parseType, isPointer };
}

// SQL expression that reads a scalar JSON value, typed for its use.
//   purpose: 'group' | 'measureNumeric' | 'compare' | 'date'
function jsonScalar(resolved, purpose) {
  const path = `'$.${resolved.jsonKey}'`;
  const col = `${resolved.sqlAlias}."JSON_DOCUMENT"`;
  if (resolved.isPointer) {
    // Stored as "ClassName$objectId" — extract the objectId portion.
    const raw = `JSON_VALUE(${col}, ${path})`;
    return `SUBSTR(${raw}, INSTR(${raw}, '$') + 1)`;
  }
  if (resolved.parseType === 'Date') {
    if (purpose === 'date') return `JSON_VALUE(${col}, ${path} RETURNING TIMESTAMP)`;
    // min/max/compare on a date: ISO strings sort chronologically, keep as text.
    return `JSON_VALUE(${col}, ${path})`;
  }
  if (resolved.parseType === 'Number' || purpose === 'measureNumeric') {
    return `JSON_VALUE(${col}, ${path} RETURNING NUMBER)`;
  }
  return `JSON_VALUE(${col}, ${path})`;
}

// ---------------------------------------------------------------------------
// Bind management
// ---------------------------------------------------------------------------

function makeBinder() {
  const binds = {};
  let n = 0;
  return {
    add(value) {
      const name = `b${n++}`;
      binds[name] = value;
      return `:${name}`;
    },
    addLimit(value) {
      const name = `b${n++}`;
      binds[name] = value; // plain JS number -> node-oracledb binds as NUMBER
      return `:${name}`;
    },
    binds,
  };
}

// Format a JS Date / ISO string into the canonical Oracle UTC timestamp text.
// All date math is UTC (see module + metrics time-zone notes).
function toOracleTimestampText(value) {
  let d;
  if (value instanceof Date) d = value;
  else if (typeof value === 'string') d = new Date(value);
  else if (value && typeof value === 'object' && value.__type === 'Date' && value.iso) d = new Date(value.iso);
  else fail('date filter value must be a Date or ISO string');
  if (isNaN(d.getTime())) fail('invalid date filter value');
  return d.toISOString().replace('Z', ''); // YYYY-MM-DDTHH:MM:SS.mmm (UTC)
}

const TS_FMT = `'YYYY-MM-DD"T"HH24:MI:SS.FF'`;

// ---------------------------------------------------------------------------
// SQL clause builders
// ---------------------------------------------------------------------------

// Validate an optional IANA time zone (dateTrunc/datePart). Missing == UTC (the
// historical behavior). Present-but-unknown is rejected. Uses Intl (the tz rule
// source) so DST / historical rules are honored — never a numeric offset.
function normalizeTimeZone(timeZone) {
  if (timeZone == null || timeZone === '') return undefined;
  if (typeof timeZone !== 'string') fail('timeZone must be an IANA string, e.g. "Europe/Rome"');
  try {
    new Intl.DateTimeFormat('en-US', { timeZone });
  } catch (e) {
    fail(`invalid IANA time zone: ${timeZone}`);
  }
  return timeZone;
}

// Safe SQL string literal for a validated zone (single quotes doubled; the zone
// is Intl-validated so it can't legitimately contain quotes — defense in depth).
function tzLiteral(timeZone) {
  return `'${String(timeZone).replace(/'/g, "''")}'`;
}

// A Date-typed JSON scalar rendered in the client zone. The stored value is a UTC
// TIMESTAMP; FROM_TZ(..,'UTC') AT TIME ZONE '<zone>' shifts it to the zone, then
// CAST(.. AS TIMESTAMP) drops the offset so the value holds the LOCAL wall clock.
// (Necessary because EXTRACT/TO_CHAR over a TIMESTAMP WITH TIME ZONE normalize the
// time fields back to UTC — casting to a plain TIMESTAMP is what keeps local
// day/hour buckets.) So TRUNC(day)/EXTRACT(hour) below bucket by what the client sees.
function zonedDateExpr(resolved, timeZone) {
  const utcTs = jsonScalar(resolved, 'date');
  if (!timeZone) return utcTs;
  return `CAST((FROM_TZ(${utcTs}, 'UTC') AT TIME ZONE ${tzLiteral(timeZone)}) AS TIMESTAMP)`;
}

function buildGroupExpr(g, ctx) {
  if (!g || typeof g !== 'object') fail('groupBy entry must be an object');
  if (!g.as || !SAFE_IDENT.test(g.as)) fail(`invalid groupBy alias: ${g.as}`);
  const resolved = resolveField(g.field, ctx);
  if (g.op === 'dateTrunc') {
    if (!DATE_UNITS.has(g.unit)) fail(`unsupported dateTrunc unit: ${g.unit}`);
    if (resolved.parseType !== 'Date') fail(`dateTrunc requires a Date field: ${g.field}`);
    const tz = normalizeTimeZone(g.timeZone);
    // 'day' -> YYYY-MM-DD text (UTC by default, else in the client zone)
    if (tz) return `TO_CHAR(${zonedDateExpr(resolved, tz)}, 'YYYY-MM-DD')`;
    return `TO_CHAR(TRUNC(${jsonScalar(resolved, 'date')}), 'YYYY-MM-DD')`;
  }
  if (g.op === 'datePart') {
    if (!DATE_PARTS.has(g.part)) fail(`unsupported datePart: ${g.part}`);
    if (resolved.parseType !== 'Date') fail(`datePart requires a Date field: ${g.field}`);
    const tz = normalizeTimeZone(g.timeZone);
    return `EXTRACT(HOUR FROM ${zonedDateExpr(resolved, tz)})`;
  }
  if (g.op) fail(`unsupported groupBy op: ${g.op}`);
  return jsonScalar(resolved, 'group');
}

function buildMeasureExpr(m, ctx, binder) {
  if (!m || typeof m !== 'object') fail('measure entry must be an object');
  if (!m.as || !SAFE_IDENT.test(m.as)) fail(`invalid measure alias: ${m.as}`);
  if (!MEASURE_OPS.has(m.op)) fail(`unsupported measure op: ${m.op}`);

  if (m.op === 'count') {
    if (m.where) {
      // Safe conditional bucket: COUNT over a CASE with a validated predicate.
      return `COUNT(CASE WHEN ${buildPredicate(m.where, ctx, binder)} THEN 1 END)`;
    }
    return 'COUNT(*)';
  }

  const resolved = resolveField(m.field, ctx);
  if (m.op === 'countDistinct') {
    return `COUNT(DISTINCT ${jsonScalar(resolved, 'group')})`;
  }
  if (ARRAY_LENGTH_OPS[m.op]) {
    if (resolved.isPointer) fail(`${m.op} cannot be applied to a Pointer field: ${m.field}`);
    // Per-row array length via Oracle's '$.field.size()' (NULL/missing -> 0), then aggregate.
    const lenExpr = `NVL(JSON_VALUE(${resolved.sqlAlias}."JSON_DOCUMENT", '$.${resolved.jsonKey}.size()' RETURNING NUMBER), 0)`;
    return `${ARRAY_LENGTH_OPS[m.op]}(${lenExpr})`;
  }
  if (NUMERIC_MEASURE_OPS.has(m.op)) {
    if (resolved.parseType !== 'Number') fail(`${m.op} requires a Number field: ${m.field}`);
    return `${m.op.toUpperCase()}(${jsonScalar(resolved, 'measureNumeric')})`;
  }
  // min / max — Number or Date (date kept as text, ISO sorts chronologically)
  const expr = resolved.parseType === 'Number' ? jsonScalar(resolved, 'measureNumeric') : jsonScalar(resolved, 'compare');
  return `${m.op.toUpperCase()}(${expr})`;
}

// A single validated comparison predicate (used by filters and buckets).
function buildPredicate(p, ctx, binder) {
  if (!p || typeof p !== 'object') fail('predicate must be an object');

  // Compound predicates: { and: [...] } / { or: [...] } (recursive, validated).
  if (Array.isArray(p.and)) {
    if (p.and.length === 0) fail('"and" requires a non-empty array');
    return '(' + p.and.map(x => buildPredicate(x, ctx, binder)).join(' AND ') + ')';
  }
  if (Array.isArray(p.or)) {
    if (p.or.length === 0) fail('"or" requires a non-empty array');
    return '(' + p.or.map(x => buildPredicate(x, ctx, binder)).join(' OR ') + ')';
  }

  const resolved = resolveField(p.field, ctx);

  const cmp = Object.keys(p).find(k => k !== 'field');
  if (!cmp || !COMPARATORS.has(cmp)) fail(`unsupported comparator in predicate: ${cmp}`);
  const value = p[cmp];

  return comparison(resolved, cmp, value, binder);
}

function comparison(resolved, cmp, value, binder) {
  const isDate = resolved.parseType === 'Date';
  const sqlOp = { eq: '=', ne: '!=', lt: '<', lte: '<=', gt: '>', gte: '>=' }[cmp];

  if (cmp === 'in') {
    if (!Array.isArray(value) || value.length === 0) fail('"in" requires a non-empty array');
    if (value.length > 1000) fail('"in" list too large');
    const placeholders = value.map(v => (isDate ? dateBind(v, binder) : binder.add(scalarBindValue(resolved, v))));
    const lhs = isDate ? jsonScalar(resolved, 'date') : jsonScalar(resolved, 'group');
    return `${lhs} IN (${placeholders.join(', ')})`;
  }

  if (isDate) {
    return `${jsonScalar(resolved, 'date')} ${sqlOp} ${dateBind(value, binder)}`;
  }
  if (resolved.parseType === 'Number') {
    return `${jsonScalar(resolved, 'measureNumeric')} ${sqlOp} ${binder.add(Number(value))}`;
  }
  return `${jsonScalar(resolved, 'group')} ${sqlOp} ${binder.add(String(value))}`;
}

function scalarBindValue(resolved, v) {
  if (resolved.parseType === 'Number') return Number(v);
  return String(v);
}

function dateBind(value, binder) {
  return `TO_TIMESTAMP(${binder.add(toOracleTimestampText(value))}, ${TS_FMT})`;
}

function buildWhere(filter, ctx, binder) {
  if (!filter) return '';
  if (typeof filter !== 'object') fail('filter must be an object');
  const clauses = [];
  for (const field of Object.keys(filter)) {
    const cond = filter[field];
    if (cond !== null && typeof cond === 'object' && !(cond instanceof Date) && !Array.isArray(cond) && !cond.__type) {
      // operator object: { $gte, $lt, ... }
      for (const opKey of Object.keys(cond)) {
        const cmp = FILTER_OPS[opKey];
        if (!cmp) fail(`unsupported filter operator: ${opKey}`);
        const resolved = resolveField(field, ctx);
        if (cmp === 'exists') {
          const lhs = jsonScalar(resolved, resolved.parseType === 'Date' ? 'date' : 'group');
          clauses.push(cond[opKey] ? `${lhs} IS NOT NULL` : `${lhs} IS NULL`);
        } else {
          clauses.push(comparison(resolved, cmp, cond[opKey], binder));
        }
      }
    } else {
      // direct equality
      const resolved = resolveField(field, ctx);
      clauses.push(comparison(resolved, 'eq', cond, binder));
    }
  }
  return clauses.length ? 'WHERE ' + clauses.join(' AND ') : '';
}

// ---------------------------------------------------------------------------
// Pure SQL builder
// ---------------------------------------------------------------------------

// resolvedTables: { [className]: { tableName, contentColumn } } (from SODA metadata).
function buildAggregationSql(spec, resolvedTables) {
  if (!spec || typeof spec !== 'object') fail('spec must be an object');
  if (!spec.className || !SAFE_IDENT.test(spec.className)) fail(`invalid className: ${spec.className}`);
  const baseTable = resolvedTables[spec.className];
  if (!baseTable) fail(`unresolved table for class: ${spec.className}`);
  if (!SAFE_IDENT.test(baseTable.tableName)) fail(`unsafe resolved table name: ${baseTable.tableName}`);

  const baseAlias = 't0';
  const lookupsByAs = {};
  const joinSql = [];
  (spec.lookups || []).forEach((lk, i) => {
    if (!lk.as || !SAFE_IDENT.test(lk.as)) fail(`invalid lookup alias: ${lk.as}`);
    if (lookupsByAs[lk.as]) fail(`duplicate lookup alias: ${lk.as}`);
    if (!lk.foreignClass || !SAFE_IDENT.test(lk.foreignClass)) fail(`invalid lookup foreignClass: ${lk.foreignClass}`);
    const ft = resolvedTables[lk.foreignClass];
    if (!ft || !SAFE_IDENT.test(ft.tableName)) fail(`unresolved/unsafe table for lookup class: ${lk.foreignClass}`);
    lookupsByAs[lk.as] = { sqlAlias: `t${i + 1}`, foreignSchema: lk.foreignSchema, tableName: ft.tableName };
  });

  const ctx = { baseAlias, baseSchema: spec.schema, lookupsByAs };

  // Resolve the local pointer for each lookup against the BASE schema only.
  (spec.lookups || []).forEach(lk => {
    const local = resolveField(lk.localField, { ...ctx, lookupsByAs: {} });
    if (!local.isPointer) fail(`lookup localField must be a Pointer: ${lk.localField}`);
    const li = lookupsByAs[lk.as];
    const localIdExpr = jsonScalar(local, 'group'); // extracts objectId
    const foreignIdExpr = `JSON_VALUE(${li.sqlAlias}."JSON_DOCUMENT", '$._id')`;
    joinSql.push(`LEFT JOIN "${li.tableName}" ${li.sqlAlias} ON ${foreignIdExpr} = ${localIdExpr}`);
  });

  const binder = makeBinder();

  // SELECT list: group expressions (aliased) + measures (aliased).
  const select = [];
  const groupExprs = [];
  const aliasUsed = new Set();
  (spec.groupBy || []).forEach(g => {
    const expr = buildGroupExpr(g, ctx);
    if (aliasUsed.has(g.as)) fail(`duplicate output alias: ${g.as}`);
    aliasUsed.add(g.as);
    select.push(`${expr} AS "${g.as}"`);
    groupExprs.push(expr);
  });

  const measures = spec.measures || [];
  if (measures.length === 0) fail('at least one measure is required');
  measures.forEach(m => {
    const expr = buildMeasureExpr(m, ctx, binder);
    if (aliasUsed.has(m.as)) fail(`duplicate output alias: ${m.as}`);
    aliasUsed.add(m.as);
    select.push(`${expr} AS "${m.as}"`);
  });

  const whereSql = buildWhere(spec.filter, ctx, binder);

  // ORDER BY references output aliases only (validated).
  const orderSql = [];
  (spec.orderBy || []).forEach(o => {
    if (!o.key || !aliasUsed.has(o.key)) fail(`orderBy key is not a selected alias: ${o.key}`);
    const dir = String(o.direction || 'asc').toLowerCase() === 'desc' ? 'DESC' : 'ASC';
    orderSql.push(`"${o.key}" ${dir} NULLS LAST`);
  });

  const limit = clampLimit(spec.limit);

  let sql = `SELECT ${select.join(', ')}\nFROM "${baseTable.tableName}" ${baseAlias}`;
  if (joinSql.length) sql += '\n' + joinSql.join('\n');
  if (whereSql) sql += '\n' + whereSql;
  if (groupExprs.length) sql += '\nGROUP BY ' + groupExprs.join(', ');
  if (orderSql.length) sql += '\nORDER BY ' + orderSql.join(', ');
  sql += `\nFETCH FIRST ${binder.addLimit(limit)} ROWS ONLY`;

  return { sql, binds: binder.binds, limit };
}

function clampLimit(limit) {
  let n = parseInt(limit, 10);
  if (!Number.isFinite(n) || n <= 0) n = DEFAULT_LIMIT;
  return Math.min(n, MAX_LIMIT);
}

// ---------------------------------------------------------------------------
// Executor (resolves SODA tables, runs SQL, returns plain rows)
// ---------------------------------------------------------------------------

async function resolveTables(adapter, classNames) {
  const oracledb = require('oracledb');
  await adapter.connect();
  const pool = oracledb.getPool('parse');
  const conn = await pool.getConnection();
  try {
    const soda = conn.getSodaDatabase();
    const out = {};
    for (const className of classNames) {
      if (!SAFE_IDENT.test(className)) fail(`invalid className: ${className}`);
      const coll = await soda.openCollection(className);
      if (!coll) fail(`collection does not exist: ${className}`);
      const meta = coll.metaData || {};
      out[className] = {
        tableName: meta.tableName || className,
        contentColumn: (meta.contentColumn && meta.contentColumn.name) || 'JSON_DOCUMENT',
      };
    }
    return out;
  } finally {
    await conn.close();
  }
}

async function runAggregation(adapter, spec) {
  const oracledb = require('oracledb');
  const classNames = [spec.className, ...(spec.lookups || []).map(l => l.foreignClass)];
  const resolvedTables = await resolveTables(adapter, classNames);
  const { sql, binds, limit } = buildAggregationSql(spec, resolvedTables);

  await adapter.connect();
  const pool = oracledb.getPool('parse');
  const conn = await pool.getConnection();
  try {
    const result = await conn.execute(sql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
      maxRows: limit, // hard ceiling: aggregate cardinality only, never raw docs
    });
    return result.rows || [];
  } finally {
    await conn.close();
  }
}

module.exports = {
  runAggregation,
  buildAggregationSql,
  resolveTables,
  clampLimit,
  toOracleTimestampText,
  AggregationError,
  MAX_LIMIT,
  DEFAULT_LIMIT,
};
