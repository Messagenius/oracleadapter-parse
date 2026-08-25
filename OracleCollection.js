// Copyright (c) 2023, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import Parse from 'parse/node';
import logger from '../../../logger.js';
import _ from 'lodash';
import OracleStorageAdapter from './OracleStorageAdapter';

const oracledb = require('oracledb');
// SODA operations honor only this GLOBAL flag (no per-call override), so
// batching several writes into one transaction requires it to be false and
// every write path to commit explicitly. AWR showed 2.48M single-row commits
// under load with the old `autoCommit = true`. Write paths below either
// commit their own connection or defer to the enclosing transactional
// session (parse-server batch {"transaction": true}).
//
// PARSE_ORACLE_AUTOCOMMIT=true restores the pre-6.8.0 behavior: every SODA
// DML commits with the statement, so a write can never outlive its request
// and no row lock can survive an idle session. It costs the commit batching
// (and with it the atomicity of transactional sessions -- set
// MSG_DB_TRANSACTIONS=false alongside it, never one without the other), but
// leaves every other pool optimization in place. Kept as an operational
// escape hatch for deployments that hit lock contention.
oracledb.autoCommit = process.env.PARSE_ORACLE_AUTOCOMMIT === 'true';
const Collection = oracledb.SodaCollection;
const SodaDB = oracledb.SodaDB;

const DB_VERSION = process.env.ORACLEDB_VERSION;
const ddlTimeOut = `
BEGIN
    EXECUTE IMMEDIATE ('
    alter session set ddl_lock_timeout=1000
    ');
END;`;

export default class OracleCollection {
  _oracleSodaDB: SodaDB;
  _oracleCollection: Collection;
  _oracleStorageAdapter: OracleStorageAdapter;
  _name: string;
  indexes = new Array();
  idIndexCreating = false;
  jsonSQLtype = 'JSON'; //DBVersion 23c default

  constructor(oracleStorageAdapter: OracleStorageAdapter, collectionName: String) {
    this._oracleStorageAdapter = oracleStorageAdapter;
    this._name = collectionName;
    this._oracleCollection = undefined;
    logger.verbose('Oracle Database Version = ' + DB_VERSION);
    // To support backwards compatibility with instant clients
    if (typeof DB_VERSION !== 'undefined' && DB_VERSION !== '23') {
      this.jsonSQLtype = 'BLOB';
    }
  }

  /*
    Create the collection (and the indexes Mongo would have created
    implicitly) on the given SodaDatabase. Takes the SodaDatabase explicitly
    so the caller decides which connection the DDL runs on -- SODA DDL commits
    independently of any surrounding transaction, so it is safe to run on a
    transactional session's own connection.
  */
  async _createCollection(sodadb) {
    const mymetadata = {
      keyColumn: { name: 'ID', assignmentMethod: 'UUID' },
      contentColumn: { name: 'JSON_DOCUMENT', sqlType: this.jsonSQLtype },
      versionColumn: { name: 'VERSION', method: 'UUID' },
      lastModifiedColumn: { name: 'LAST_MODIFIED' },
      creationTimeColumn: { name: 'CREATED_ON' },
    };

    logger.verbose('_createCollection create NEW collection for  ' + this._name);
    const newCollection = await sodadb.createCollection(this._name, {
      metaData: mymetadata,
    });

    /*
      Create index on _id for every new collection
      This imitates Mongo behavior which happens automatically

      Index names MUST be unique in a schema, append table name
      cannot have two indexes with the same name in a single schema.
    */
    if (!this.idIndexCreating) {
      this.idIndexCreating = true;
      const indexName = 'ididx' + this._name;
      const indexSpec = { name: indexName, unique: true, fields: [{ path: '_id' }] };
      await newCollection.createIndex(indexSpec);
      logger.verbose('_createCollection successfully create _id index for  ' + this._name);
      // Add _id if it doesn't exist to indexes array
      const found = this.indexes.find(item => {
        return Object.keys(item)[0] === '_id_';
      });
      if (typeof found === 'undefined') {
        this.indexes.push({ _id_: { _id: 1 } });
      }
    }

    // Relation join collections are queried by owningId/relatedId but
    // are invisible to schema-driven index creation — index them here.
    if (this._name.startsWith('_Join:')) {
      for (const [prefix, path] of [
        ['ownidx', 'owningId'],
        ['relidx', 'relatedId'],
      ]) {
        try {
          await newCollection.createIndex({
            name: prefix + this._name,
            fields: [{ path: path }],
          });
        } catch (error) {
          if (error.errorNum !== 40733) {
            throw error;
          }
        }
      }
    }
    return newCollection;
  }

  async getCollectionConnection() {
    logger.verbose('getCollectionConnection about to connect for collection ' + this._name);
    let localConn;
    let localSodaDB;
    this._oracleCollection = await this._oracleStorageAdapter
      .connect()
      .then(p => {
        logger.verbose('getCollectionConnection about to get connection from pool ');
        logger.verbose('  statistics: ' + JSON.stringify(p.getStatistics()));
        return p.getConnection();
      })
      .then(conn => {
        logger.verbose('getCollectionConnection about to get SodaDB');
        localConn = conn;
        return conn.getSodaDatabase();
      })
      .then(sodadb => {
        logger.verbose('getCollectionConnection open collection for  ' + this._name);
        // Keep the SodaDatabase in a local: this._oracleSodaDB is shared
        // mutable state and a concurrent call may re-bind it to another
        // connection before the next .then() runs.
        localSodaDB = sodadb;
        this._oracleSodaDB = sodadb;
        return sodadb.openCollection(this._name);
      })
      .then(async coll => {
        if (!coll) {
          return this._createCollection(localSodaDB);
        }
        return coll;
      })
      .catch(async error => {
        logger.error('getCollectionConnection ERROR:  ' + error);
        // Release the pool connection before propagating. Without this any
        // failure after getConnection() -- openCollection, createCollection,
        // createIndex, a DDL lock timeout -- leaks the session for the life of
        // the process: it is checked out of the pool with nobody holding a
        // reference to close it, and with autoCommit = false it keeps any
        // transaction (and its row locks) open indefinitely.
        if (localConn) {
          try {
            await localConn.close();
          } catch (closeError) {
            logger.error('getCollectionConnection close error: ' + closeError);
          }
          localConn = null;
        }
        throw error;
      });
    logger.verbose(
      'getCollectionConnection returning collection for  ' +
        this._name +
        ' returned ' +
        this._oracleCollection
    );
    return localConn;
  }

  /*
    Write-context helpers.

    With the global oracledb.autoCommit = false, every write path must either
    commit its own pool connection or run on the connection of an enclosing
    transactional session (created by OracleStorageAdapter.
    createTransactionalSession) and let the session owner commit/rollback.

    A context is { conn, collection, transactional }:
    - transactional=false: conn came from the pool via getCollectionConnection,
      caller must _commitWrite() on success and _closeWriteContext() always.
    - transactional=true: conn/collection belong to the session; commit,
      rollback and close are the session owner's job — both helpers no-op.
  */
  async _writeContext(transactionalSession) {
    if (transactionalSession && transactionalSession.conn) {
      if (!transactionalSession.collections) {
        transactionalSession.collections = {};
      }
      let collection = transactionalSession.collections[this._name];
      if (!collection) {
        collection = await transactionalSession.conn.getSodaDatabase().openCollection(this._name);
        if (!collection) {
          // Collection does not exist yet. Create it on the session's OWN
          // connection: going through getCollectionConnection would take a
          // second connection from the pool while this session already holds
          // one (with locks), which is the same pool self-deadlock updateMany
          // used to have. SODA DDL commits independently of the surrounding
          // transaction, so creating it here does not affect the batch.
          collection = await this._createCollection(
            transactionalSession.conn.getSodaDatabase()
          );
        }
        transactionalSession.collections[this._name] = collection;
      }
      return { conn: transactionalSession.conn, collection: collection, transactional: true };
    }
    const conn = await this.getCollectionConnection();
    // Open the collection on OUR connection rather than reading
    // this._oracleCollection. That field lives on a process-wide cached
    // OracleCollection (see OracleStorageAdapter._adaptiveCollection) and is
    // re-bound by every concurrent getCollectionConnection call, so reading it
    // here can hand back a handle bound to another request's session. The
    // replaceOne then runs on that session while _commitWrite/_closeWriteContext
    // act on this one -- with oracledb.autoCommit = false that strands an
    // uncommitted row lock on a connection nobody will commit. The
    // transactional branch above already opens on its own connection; do the
    // same here. sodaMetaDataCache makes this a local lookup, not a round trip.
    const collection = await conn.getSodaDatabase().openCollection(this._name);
    if (!collection) {
      // getCollectionConnection creates the collection when missing (DDL, which
      // commits independently), so this should be unreachable. Fail loudly
      // rather than returning a context with a null collection.
      await conn.close();
      throw new Error('could not open collection ' + this._name + ' on its own connection');
    }
    return { conn: conn, collection: collection, transactional: false };
  }

  async _commitWrite(ctx) {
    if (!ctx.transactional) {
      await ctx.conn.commit();
    }
  }

  async _closeWriteContext(ctx) {
    if (!ctx.transactional && ctx.conn) {
      try {
        await ctx.conn.close();
      } catch (error) {
        logger.error('error closing write connection: ' + error);
      }
      ctx.conn = null;
    }
  }

  // Atomically updates data in the database for a single (first) object that matched the query
  // If there is nothing that matches the query - does insert
  // Postgres Note: `INSERT ... ON CONFLICT UPDATE` that is available since 9.5.
  async upsertOne(query, update, session) {
    /*
      UpsertOne is of the form
      where query =
      {"_id": "HasAllPOD"}
      and update = the new document
      {"_id": "HasAllPOD","numPODs": 17"}

      in this case if update fails becuase no document existed then
      rerunning the query would return 0 and indicate an insert
    */

    logger.verbose('in upsertOne query = ' + JSON.stringify(query));
    logger.verbose('use session to make linter happy ' + JSON.stringify(session));
    // TODO need to use save(), which is the SODA equivalent of upsert() andit takes a SodaDocument
    let docs;
    let promise;

    try {
      promise = await this.findOneAndUpdate(query, update, session);
      logger.verbose('Upsert Promise = ' + promise);
      if (promise === false) {
        logger.verbose('Upsert Insert for query ' + JSON.stringify(query));
        promise = await this._rawFind(query, { type: 'sodadocs' }).then(d => (docs = d));
        if (docs && docs.length == 0) {
          // Its an insert so merge query into update
          _.merge(update, query);
          promise = await this.insertOne(update, session);
        }
      }
      return promise;
    } catch (error) {
      logger.error('Collection UpsertOne throws ' + error);
      throw error;
    }
  }

  async findOneAndUpdate(query, update, transactionalSession) {
    try {
      logger.verbose('in Collection findOneAndUpdate query = ' + JSON.stringify(query));
      logger.verbose(
        'use transactionalSession to make linter happy ' + JSON.stringify(transactionalSession)
      );

      // TODO:  Fix updatedAt, it should be _updatedAt because its an internal field
      //              and updatedAt doesn't get updated for Schemas

      let updateObj;

      let result = await this._rawFind(query, { type: 'one' }).then(result => {
        return result;
      });
      //************************************************************************************************/
      // Modify Update based on Mongo operators
      //
      // Look for $unset, Mongo's deleteField
      // Create array of fieldNames to be deleted
      const newUpdate = new Object();
      const fieldNames = new Array();
      Object.keys(update).forEach(item => {
        if (item === '$unset') {
          Object.keys(update[item]).forEach(item => {
            fieldNames.push(item);
          });
        } else {
          if (item === '_updated_at') {
            newUpdate['updatedAt'] = update[item];
          } else {
            newUpdate[item] = update[item];
          }
        }
      });

      // If $unset was sent, strip those keys off the in-memory copy of the
      // single matched document and continue. Do NOT call a collection-wide
      // delete here: Parse sends $unset for per-row clears (field.unset(),
      // pointer null-outs, after-trigger cleanups) and a collection-wide
      // strip wipes the field from every document, not just this row.
      if (fieldNames.length > 0) {
        update = newUpdate;
        if (result && result.content) {
          fieldNames.forEach(fieldName => _.unset(result.content, fieldName));
        }
      }

      // Process Increments  $inc
      const newIncUpdate = new Object();
      let incUpdt = false;
      Object.keys(update).forEach(item => {
        if (item === '$inc') {
          Object.keys(update[item]).forEach(it2 => {
            incUpdt = true;
            _.set(result.content, it2, _.result(result.content, it2) + update[item][it2]);
          });
        } else {
          if (item === '_updated_at') {
            newIncUpdate['updatedAt'] = update[item];
          } else {
            newIncUpdate[item] = update[item];
          }
        }
      });

      if (incUpdt) {
        update = newIncUpdate;
      }

      // Process $AddToSet operator adds a value to an array unless the value is already present, in which case $addToSet does nothing to that array.
      const newAddToSetUpdate = new Object();
      let addToSetUpdt = false;
      Object.keys(update).forEach(item => {
        if (item === '$addToSet') {
          Object.keys(update[item]).forEach(it2 => {
            Object.keys(update[item][it2]).forEach(it3 => {
              if (it3 === '$each') {
                const updtArray = update[item][it2][it3];
                // Check for dot notation
                const temp = it2.split('.');
                let newArray;
                if (temp.length > 1) {
                  newArray = result.content[temp[0]][temp[1]];
                } else {
                  newArray = result.content[it2];
                }
                updtArray.forEach(updt => {
                  if (typeof updt === 'object') {
                    if (!newArray.some(entry => Object.keys(entry)[0] === Object.keys(updt)[0])) {
                      addToSetUpdt = true;
                      newArray.push(updt);
                    }
                  } else {
                    if (!newArray.includes(updt)) {
                      addToSetUpdt = true;
                      newArray.push(updt);
                    }
                  }
                });
              }
            });
          });
        } else {
          if (item === '_updated_at') {
            newAddToSetUpdate['updatedAt'] = update[item];
          } else {
            newAddToSetUpdate[item] = update[item];
          }
        }
      });

      if (addToSetUpdt) {
        update = newAddToSetUpdate;
      }

      // Process $pullAll operator removes all instances of the specified values from an existing array.
      const newPullAllUpdate = new Object();
      let pullAllUpdt = false;
      Object.keys(update).forEach(item => {
        if (item === '$pullAll') {
          Object.keys(update[item]).forEach(it2 => {
            const updtArray = update[item][it2];
            const rsltArray = result.content[it2];
            const newArray = new Array();
            updtArray.forEach(updt => {
              if (typeof updt === 'object') {
                rsltArray.forEach(entry => {
                  if (Object.keys(entry)[0] != Object.keys(updt)[0]) {
                    newArray.push(entry);
                    pullAllUpdt = true;
                  }
                });
              }
              newPullAllUpdate[it2] = newArray;
            });
          });
        } else {
          if (item === '_updated_at') {
            newPullAllUpdate['updatedAt'] = update[item];
          } else {
            newPullAllUpdate[item] = update[item];
          }
        }
      });

      if (pullAllUpdt) {
        update = newPullAllUpdate;
      }

      // End of Transform Update
      //************************************************************************************************/

      if (result && Object.keys(result).length > 0) {
        // found the doc, so we need to update it
        const key = result.key;
        logger.verbose('key = ' + key);
        const version = result.version;
        logger.verbose('version = ' + version);
        const oldContent = result.content;

        logger.verbose('oldContent = ' + JSON.stringify(oldContent));
        logger.verbose('update = ' + JSON.stringify(update));

        // Note: previously this block treated `{ foo: {} }` as an unset of
        // `foo`. That's wrong: `_.merge(oldContent, { foo: {} })` is already
        // a no-op for that field (matching Mongo). Genuine clears arrive
        // via the $unset operator, which is handled above. Silently
        // unsetting on empty objects deleted user data when callers passed
        // pristine sub-objects through transforms that hadn't yet been
        // populated.
        if (update.fieldName) {
          const theUpdate = { [update.fieldName]: update.theFieldType };
          logger.verbose('theUpdate = ' + JSON.stringify(theUpdate));
          updateObj = { ...oldContent, ...theUpdate };
        } else {
          if (pullAllUpdt || update['_metadata']) {
            // Handle set or merge for _metadata in Schema
            Object.keys(update).forEach(item => {
              const found = Object.keys(oldContent).find(item => {
                return item === '_metadata';
              });
              if (item === '_metadata') {
                if (found) {
                  if (
                    Object.prototype.hasOwnProperty.call(oldContent[item], 'class_permissions') &&
                    Object.prototype.hasOwnProperty.call(update[item], 'class_permissions')
                  ) {
                    // Just reset class_permissions to update
                    _.set(oldContent[item], 'class_permissions', update[item]['class_permissions']);
                  } else {
                    _.merge(oldContent['_metadata'], update[item]);
                  }
                } else {
                  _.set(oldContent, item, update[item]);
                }
              } else {
                _.set(oldContent, item, update[item]);
              }
            });
            updateObj = oldContent;
          } else {
            // Use mergeWith with an array customizer so that arrays in `update`
            // fully replace arrays in `oldContent`. Plain `_.merge` merges
            // arrays by index, which silently preserves trailing elements when
            // the new array is shorter — corrupting any field whose update
            // shrinks an array (e.g. participantsIds when removing a user).
            updateObj = _.mergeWith(oldContent, update, (objVal, srcVal) =>
              Array.isArray(srcVal) ? srcVal : undefined
            );
          }
        }
        logger.verbose('Updated Object = ' + JSON.stringify(updateObj));
        const ctx = await this._writeContext(transactionalSession);
        try {
          const replaceResult = await ctx.collection
            .find()
            .key(key)
            .version(version)
            .replaceOne(updateObj);
          if (replaceResult.replaced == true) {
            await this._commitWrite(ctx);
            return updateObj;
          }
          return 'retry';
        } catch (error) {
          logger.error('Find One and Update replaceOne ERROR = ', error);
          throw error;
        } finally {
          await this._closeWriteContext(ctx);
        }
      } else {
        logger.verbose('No Docs, nothing to update, return false');
        return false;
      }
    } catch (error) {
      logger.error('Find One and Update ERROR = ', error);
      throw error;
    }
  }

  async updateSchemaIndexes(query, update) {
    // This method just updates Schema _metadata.indexes
    // It is laways a set (replace), never a merge
    logger.verbose('in Collection updateSchemaIndexes query = ' + JSON.stringify(query));
    logger.verbose('update = ' + JSON.stringify(update));
    const result = await this._rawFind(query, { type: 'one' }).then(result => {
      return result;
    });
    if (Object.keys(result).length > 0) {
      // found the doc, so we need to update it
      const key = result.key;
      logger.verbose('key = ' + key);
      const version = result.version;
      logger.verbose('version = ' + version);
      const oldContent = result.content;
      logger.verbose('oldContent = ' + JSON.stringify(oldContent));
      logger.verbose('update = ' + JSON.stringify(update));
      // Either set or merge _metadata depending on if it existed before
      Object.keys(update).forEach(item => {
        if (item === '_metadata') {
          if (Object.prototype.hasOwnProperty.call(oldContent, item)) {
            if (Object.prototype.hasOwnProperty.call(oldContent[item], 'indexes')) {
              if (
                Object.keys(update).length <= Object.keys(oldContent['_metadata']['indexes']).length
              ) {
                // Its a delete.  Parse deletes by sending an update with the deleted index
                // Set Indexes w Update only
                _.set(oldContent[item], 'indexes', update[item]['indexes']);
              } else {
                _.merge(oldContent['_metadata'], update[item]);
              }
            } else {
              _.merge(oldContent['_metadata'], update[item]);
            }
          } else {
            _.set(oldContent, item, update[item]);
          }
        }
      });
      const updateObj = oldContent;
      logger.verbose('Updated Object = ' + JSON.stringify(updateObj));

      const ctx = await this._writeContext(null);
      try {
        const replaceResult = await ctx.collection
          .find()
          .key(key)
          .version(version)
          .replaceOne(updateObj);
        if (replaceResult.replaced == true) {
          await this._commitWrite(ctx);
          return update;
        }
        return 'retry';
      } catch (error) {
        logger.error('updateSchemaIndexes update ERROR: ', error);
        throw error;
      } finally {
        await this._closeWriteContext(ctx);
      }
    } else {
      logger.verbose('updateSchemaIndexes No record found for query: ' + JSON.stringify(query));
      return false;
    }
  }
  catch(error) {
    logger.error('updateSchemaIndexes ERROR: ', error);
    throw error;
  }

  async findOneAndDelete(query: string) {
    try {
      logger.verbose('in Collection findOneAndDelete query = ' + JSON.stringify(query));

      const result = await this._rawFind(query, { type: 'one' }).then(result => {
        return result;
      });

      if (Object.keys(result).length > 0) {
        // found the doc, so we need to update it
        const key = result.key;
        logger.verbose('key = ' + key);
        const version = result.version;
        logger.verbose('version = ' + version);

        const ctx = await this._writeContext(null);
        try {
          const removeResult = await ctx.collection.find().key(key).version(version).remove();
          await this._commitWrite(ctx);
          return removeResult;
        } catch (error) {
          logger.error('Find One and Delete remove ERROR: ', error);
          throw error;
        } finally {
          await this._closeWriteContext(ctx);
        }
      } else {
        logger.verbose('Find One and Delete No record found for query: ' + JSON.stringify(query));
      }
    } catch (error) {
      logger.error('Find One and Delete ERROR: ', error);
      throw error;
    }
  }

  async deleteObjectsByQuery(query, transactionalSession) {
    try {
      logger.verbose('in Collection deleteObjectsByQuery query = ' + JSON.stringify(query));
      logger.verbose(
        'use transactionalSession to make linter happy ' + JSON.stringify(transactionalSession)
      );

      const result = await this._rawFind(query, { type: 'all' }).then(result => {
        return result;
      });

      if (result.length > 0) {
        // One connection, one commit for the whole matched set. The previous
        // code returned from inside the loop and silently deleted only the
        // FIRST matching document (with a commit per row on top).
        const ctx = await this._writeContext(transactionalSession);
        try {
          let removed = 0;
          for (let i = 0; i < result.length; i++) {
            const key = result[i].key;
            const version = result[i].version;
            const removeResult = await ctx.collection
              .find()
              .key(key)
              .version(version)
              .remove();
            removed += removeResult.count;
          }
          await this._commitWrite(ctx);
          logger.verbose('deleteObjectsByQuery removed ' + removed + ' of ' + result.length);
          return removed;
        } catch (error) {
          logger.error('Delete Objects By Query remove ERROR: ', error);
          throw error;
        } finally {
          await this._closeWriteContext(ctx);
        }
      } else {
        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
      }
    } catch (error) {
      logger.error('Delete Objects By Query ERROR: ', error);
      throw error;
    }
  }

  // Delete fields from all documents in a collection.
  // ONLY safe for schema-level field drops (StorageAdapter.deleteFields,
  // i.e. the _SCHEMA "remove this column for the whole class" API). Do
  // NOT call from per-row update paths: a per-row $unset must touch only
  // the matched row, never sweep the whole collection.
  async deleteFields(fieldNames: Array<string>) {
    try {
      var promises = Array();
      // Rewriting like createIndexes, Collection method will just delete a field
      logger.verbose(
        'DeleteFields ' + JSON.stringify(fieldNames) + ' for Collection ' + this._name
      );
      for (let idx = 0; idx < fieldNames.length; idx++) {
        const fieldName = fieldNames[idx];
        logger.verbose('about to delete field' + fieldName);
        const promise = this.deleteFieldFromCollection(fieldName)
          .then(promise => {
            if (promise === 'retry') {
              return this.deleteFieldFromCollection(fieldName);
            }
            return promise;
          })
          .catch(error => {
            logger.error('Collection deleteFields caught error ' + error.message);
            throw error;
          });
        promises.push(promise);
      }

      const results = await Promise.all(promises);
      logger.verbose('DeleteFields returns ' + results);
      return results;
    } catch (error) {
      logger.error('Delete Fields ERROR: ', error);
      throw error;
    }
  }

  // deleteField from all docs in a collection that has it
  async deleteFieldFromCollection(fieldName: string) {
    try {
      logger.verbose('deleteFieldFromCollection fieldName to delete is ' + fieldName);
      const query = JSON.parse(`{"${fieldName}":{"$exists":true}}`);
      const result = await this._rawFind(query, { type: 'all' }).then(result => {
        return result;
      });

      if (result.length > 0) {
        // found the doc, so we need to update it
        var promises = Array();
        for (let i = 0; i < result.length; i++) {
          const promise = this.deleteField(
            fieldName,
            result[i].key,
            result[i].version,
            result[i].content
          )
            .then(promise => {
              if (promise === 'retry') {
                return this.deleteFieldFromCollection(fieldName);
              }
              return promise;
            })
            .catch(error => {
              logger.error('deleteFieldFromConnection caught error ' + error.message);
              throw error;
            });
          promises.push(promise);
        }

        const results = await Promise.all(promises);
        logger.verbose('DeleteFieldFromCollection returns ' + results);
        return results;
      } else {
        logger.verbose('Field ' + fieldName + ' Not Found In DeleteFieldFromCollection');
        return false;
      }
    } catch (error) {
      logger.error('Delete Field ERROR: ', error);
      throw error;
    }
  }

  // deleteField from a specific document containing it
  async deleteField(fieldName: string, key: string, version: string, oldContent: string) {
    logger.verbose('key = ' + key);
    logger.verbose('version = ' + version);
    logger.verbose('oldContent before delete = ' + JSON.stringify(oldContent));
    delete oldContent[fieldName];
    logger.verbose('oldContent after delete update = ' + JSON.stringify(oldContent));

    const ctx = await this._writeContext(null);
    try {
      const result = await ctx.collection.find().key(key).version(version).replaceOne(oldContent);
      if (result.replaced == true) {
        await this._commitWrite(ctx);
        return oldContent;
      }
      return 'retry';
    } catch (error) {
      logger.error('DeleteFieldFromCollection replaceOne ERROR: ', error);
      throw error;
    } finally {
      await this._closeWriteContext(ctx);
    }
  }

  //  Delete a field in a specific SCHEMA doc
  async deleteSchemaField(query: string, fieldName: string) {
    try {
      logger.verbose('fieldName to delete is ' + fieldName);
      const existobj = JSON.parse(`{"${fieldName}":{"$exists":true}}`);
      const newquery = { ...query, ...existobj };
      const result = await this._rawFind(newquery, { type: 'one' }).then(result => {
        return result;
      });

      if (result) {
        // found the doc, so we need to update it
        const key = result.key;
        logger.verbose('key = ' + key);
        const version = result.version;
        logger.verbose('version = ' + version);
        const oldContent = result.content;

        logger.verbose('oldContent before delete = ' + JSON.stringify(oldContent));
        delete oldContent[fieldName];
        logger.verbose('oldContent after delete update = ' + JSON.stringify(oldContent));

        const ctx = await this._writeContext(null);
        try {
          const replaceResult = await ctx.collection
            .find()
            .key(key)
            .version(version)
            .replaceOne(oldContent);
          if (replaceResult.replaced == true) {
            await this._commitWrite(ctx);
            return oldContent;
          }
          return 'retry';
        } catch (error) {
          logger.error('Delete SCHEMA Field replaceOne ERROR: ', error.message);
          throw error;
        } finally {
          await this._closeWriteContext(ctx);
        }
      } else {
        logger.verbose('Field ' + fieldName + ' Not Found In DeleteSchemaField');
        return false;
      }
    } catch (error) {
      logger.error('Delete SCHEMA Field ERROR: ', error);
      throw error;
    }
  }

  // Does a find with "smart indexing".
  // Currently this just means, if it needs a geoindex and there is
  // none, then build the geoindex.
  // This could be improved a lot but it's not clear if that's a good
  // idea. Or even if this behavior is a good idea.
  async find(
    query,
    {
      skip,
      limit,
      sort,
      keys,
      maxTimeMS,
      readPreference,
      hint,
      caseInsensitive,
      explain,
      sortTypes,
    } = {}
  ) {
    try {
      logger.verbose('entering find()');
      // Support for Full Text Search - $text
      if (keys && keys.$score) {
        delete keys.$score;
        keys.score = { $meta: 'textScore' };
      }

      return this._rawFind(
        query,
        { type: 'content' },
        {
          skip,
          limit,
          sort,
          keys,
          maxTimeMS,
          readPreference,
          hint,
          caseInsensitive,
          explain,
          sortTypes,
        }
      ).then(result => {
        return result;
      });
    } catch (error) {
      logger.verbose("in find()'s error block");
      // Check for "no geoindex" error
      if (error.code != 17007 && !error.message.match(/unable to find index for .geoNear/)) {
        throw error;
      }
      // Figure out what key needs an index
      const key = error.message.match(/field=([A-Za-z_0-9]+) /)[1];
      if (!key) {
        throw error;
      }
      // TODO: Need to fix up this call to DB
      // TODO:  MUST FIX
      var index = {};
      index[key] = '2d';

      // Create the geo index on a connection we actually hold and release it
      // before retrying. Previously this discarded the connection returned by
      // getCollectionConnection (leaking the session) and then called
      // this.closeConnection(), which is not a method on this class and threw
      // a TypeError.
      const conn = await this.getCollectionConnection();
      try {
        const collection = await conn.getSodaDatabase().openCollection(this._name);
        if (!collection) {
          throw new Error('could not open collection ' + this._name + ' to create geo index');
        }
        await collection.createIndex(index);
      } finally {
        await conn.close();
      }

      // Retry, but just once. _rawFind's second parameter is the return-type
      // selector, not the options bag -- passing the options there left
      // retval.type undefined, so _rawFind resolved to undefined and the
      // caller's .map() threw. Mirror the primary path above exactly.
      return await this._rawFind(
        query,
        { type: 'content' },
        {
          skip,
          limit,
          sort,
          keys,
          maxTimeMS,
          readPreference,
          hint,
          caseInsensitive,
          explain,
          sortTypes,
        }
      );
    }
  }

  // Server-side document count (SODA operation.count()); shares _rawFind's query
  // normalization and connection handling. Note: count() cannot be combined with
  // skip/limit/orderby, so no options are passed through.
  async count(query) {
    return this._rawFind(query, { type: 'count' }, {});
  }

  async _rawFind(
    query,
    retval,
    {
      skip,
      limit,
      sort,
      keys,
      maxTimeMS,
      readPreference,
      hint,
      caseInsensitive,
      explain,
      sortTypes,
    } = {}
  ) {
    logger.verbose('_rawFind: collection = ' + JSON.stringify(this._oracleCollection));
    logger.verbose('query = ' + JSON.stringify(query));
    logger.verbose('limit = ' + limit);
    // use these so the linter will not complain - until i actually use them properly
    logger.verbose(
      'TODO: not using these: ' + sort,
      maxTimeMS,
      readPreference,
      caseInsensitive,
      explain
    );

    let localConn = null;
    try {
      let findOperation;

      await this.getCollectionConnection()
        .then(async conn => {
          localConn = conn;
          // Same reason as _writeContext: this._oracleCollection is shared
          // mutable state re-bound by concurrent callers, so a find() built
          // from it can execute on another request's connection -- returning
          // that request's results, or failing if it already closed.
          const collection = await conn.getSodaDatabase().openCollection(this._name);
          if (!collection) {
            throw new Error('could not open collection ' + this._name + ' on its own connection');
          }
          findOperation = collection.find();
        })
        .catch(async error => {
          logger.error('Error getting connection in _rawFind, ERROR =' + error);
          if (localConn) {
            await localConn.close();
            localConn = null;
          }
          throw error;
        });

      //    let findOperation = this._oracleCollection.find(); // find() is sync and returns SodaOperation

      //  All this below is to handle empty array in $in selection
      //  Node APIs fail for empty array error
      //  The fix will be in a future release of instant client
      //  https://orahub.oci.oraclecorp.com/ora-microservices-dev/mbaas-parse-server/-/wikis/ORA-40676:-invalid-Query-By-Example-(QBE)-filter-specification-JZN-00305:-Array-of-values-was-empty
      const myObj = JSON.parse(JSON.stringify(query));

      for (const x in myObj) {
        if (typeof myObj[x] === 'object') {
          const json = JSON.parse(JSON.stringify(myObj[x]));

          //CDB
          //to manage EqualTo() with null
          // when an input query is like
          // {"foo":null,"$or":[{"_rperm":{"$in":["*","*"]}},{"_rperm":null},{"_rperm":{"$exists":false}}]}
          // and need to generate a $or for null check, need to wrap the whole thing with a $and
          // It looks like null = non-existance or null
          if (json == null) {
            let newQuery = {};

            if (Object.prototype.hasOwnProperty.call(myObj, '$or')) {
              // This whole not handling null is getting ugly
              const originalOr = JSON.stringify(myObj['$or']);
              const queryOr = JSON.stringify({ $or: [{ [x]: { $exists: false } }, { [x]: null }] });
              const andString = `[${queryOr},{"$or":${originalOr}}]`;
              newQuery['$and'] = JSON.parse(andString);
              delete myObj['$or'];
            } else {
              newQuery = { $or: [{ [x]: { $exists: false } }, { [x]: null }] };
            }
            query = newQuery;
          }
          //CDB-END
          //CDB
          //to manage notEqualTo() with null
          if (json != null) {
            if (Object.keys(json)[0] == '$ne') {
              if (json['$ne'] == null) {
                const newQuery = { $and: [{ [x]: { $exists: true } }, { [x]: { $ne: null } }] };
                query = newQuery;
              }
            }
          }
          //CDB-END

          // SODA QBE mis-generates the JSON path when $exists shares a field
          // with another operator. {f: {$ne: v, $exists: true}} -- which is
          // exactly what Parse produces for notEqualTo(f, v) + exists(f) --
          // becomes
          //   exists(@.f?((!(@ == $B0)) && (exists(@@))))
          // and the `@@` is not valid path syntax, so Oracle raises ORA-40597
          // / JZN-00229 and the whole query fails.
          //
          // Split the $exists into its own conjunct so each operator gets its
          // own predicate. The other fields of the query are preserved (unlike
          // the null rewrites above, which replace it wholesale).
          if (json != null && !Array.isArray(json) && !x.startsWith('$')) {
            const ops = Object.keys(json);
            if (ops.length > 1 && ops.includes('$exists')) {
              const rest = Object.assign({}, json);
              delete rest['$exists'];
              const split = [{ [x]: { $exists: json['$exists'] } }, { [x]: rest }];
              const newQuery = Object.assign({}, query);
              delete newQuery[x];
              newQuery['$and'] = Array.isArray(newQuery['$and'])
                ? newQuery['$and'].concat(split)
                : split;
              query = newQuery;
            }
          }

          //CDD
          // Remove empty objects from $and clause
          // ORA-40676: invalid Query-By-Example (QBE) filter specification
          // JZN-00315: Empty objects not allowed
          //
          // fix up queries like
          // { '$and': [ {}, { _p_user: '_User$EYTVvcG4j9' } ] }
          if (json != null && x == '$and') {
            if (Array.isArray(json)) {
              const condList = new Array();
              json.forEach(item => {
                if (!(Object.keys(item).length === 0)) {
                  condList.push(item);
                }
              });
              query = {
                $and: condList,
              };
            }
          }
          //CDD

          for (const y in json) {
            //query should not match on array when searching for null
            if (y === '$all' && Array.isArray(json[y]) && json[y][0] == null) {
              if (localConn) {
                await localConn.close();
                localConn = null;
              }
              return [];
            } else {
              // to manage $all of normal expression for query match on array with multiple objects
              if (
                y === '$all' &&
                Array.isArray(json[y]) &&
                json[y][0]['__FIELD__!!__'] === undefined
              ) {
                const newCondList = Array();

                for (var ass in myObj[x]['$all']) {
                  if (typeof myObj[x]['$all'][ass] === 'object') {
                    // ???
                    const condList = myObj[x]['$all'][0];
                    Object.keys(condList).forEach(function (key) {
                      // key: the name of the object key
                      // index: the ordinal position of the key within the object
                      const newField = x + '[*].' + key;
                      newCondList.push({
                        [newField]: condList[key],
                      });
                    });
                  }
                }
                // For 'containsAll date array queries','containsAll string array queries','containsAll number array queries'
                // no 'objects' in array: doesn't need a query re-write in $and:[] 'for query match on array with multiple objects'
                // newCondList == []
                if (newCondList.length != 0) {
                  query = {
                    $and: newCondList,
                  };
                }
              } //CDB
            }

            if (y === '$in' || y === '$nin' || y === '$all') {
              if (json[y].length > 0 && json[y][0] !== null) {
                //TO MANAGE 'containsAllStartingWith single empty value returns empty results' test
                if (
                  Object.keys(json[y][0]).length == 0 &&
                  y === '$all' &&
                  typeof json[y][0] == 'object'
                ) {
                  if (localConn) {
                    await localConn.close();
                    localConn = null;
                  }
                  return [];
                }
              }

              if (json[y].length == 0) {
                if (y === '$in' || y === '$all') {
                  if (localConn) {
                    await localConn.close();
                    localConn = null;
                  }
                  return [];
                } else {
                  query = JSON.parse('{}');
                }
              }
            }
            // to manage $all of $regex expression
            //To exclude a $all on $regex array to be transformed in $and

            /* CDD Commented this code out becuase it broke this query
               {"numbers":{"$all":[1,2,3]}
               and this test
               containsAll number array queries
               */

            /*          if (y === '$all' && json[y][0]['__FIELD__!!__'] === undefined) {
              //find wrong field
              for (ass in myObj[x]['$all']) {
                if (typeof myObj[x]['$all'][ass] === 'object') {
                  if (Object.keys(ass)[0] != '$regex') {
                    //TO BE FIXED
                    if (localConn) {
                      localConn.close();
                      localConn = null;
                    }
                    return [];
                  }
                }
              } //To manage 'containsAll number array queries' in conflict with 'containsAllStartingWith single empty value returns empty results' test
              if (localConn) {
                localConn.close();
                localConn = null;
              }
              return [];
            }*/

            if (y === '$all' && !(json[y][0]['__FIELD__!!__'] === undefined)) {
              const condList = [];

              for (const condition in query[x][y]) {
                condList.push({
                  [x]: query[x][y][condition]['__FIELD__!!__'],
                });
              }

              query = {
                $and: condList,
              };
            } //CDB-END
          }

          // Let $or just passthrough
          if (x === '$or') {
            query[x] = myObj[x];
          }
        }
      } //CDB

      if (sort && Object.keys(sort).length != 0) {
        //ADD ORDER IN QUERY
        //FIX 15-11
        const orderByList = []; //let collection = new OracleSchemaCollection(this._oracleCollection);
        for (const s in sort) {
          const order = sort[s] == -1 ? 'desc' : 'asc';
          const orderStatement = {
            path: s,
            datatype: sortTypes[s],
            order: order,
          }; //Fix 11-11

          orderByList.push(orderStatement);
        } //Fix 15-11

        const oldQuery = query;
        query = {};
        query['$query'] = oldQuery;
        query['$orderby'] = orderByList; //Fix-End 11-11
      } // CDB-END

      findOperation = findOperation.filter(query);

      if (skip) {
        findOperation = findOperation.skip(Number(skip));
      }

      if (limit) {
        findOperation = findOperation.limit(Number(limit));
      }

      if (hint) {
        findOperation = findOperation.hint(String(hint));
      }
      // TODO need to handle sort and readPreference
      // let findOperation = this._oracleCollection.find(query, {
      //   skip,
      //   limit,
      //   sort,
      //   readPreference,
      //   hint,
      // });

      if (keys) {
        logger.verbose('keys.. with input = ' + JSON.stringify(keys));
        // param needs to be an Array
        // check it is not an empty object...
        if (!_.isEmpty(keys)) {
          logger.verbose('keys was not empty');
          //CDB
          //findOperation = findOperation.keys(keys);
          //CDB-END
        }
      }

      // if (caseInsensitive) {
      //   findOperation = findOperation.collation(OracleCollection.caseInsensitiveCollation());
      // }

      // if (maxTimeMS) {
      //   findOperation = findOperation.maxTimeMS(maxTimeMS);
      // }

      // Server-side count: never materializes documents. Counting by fetching all docs
      // (the previous behavior) loads the entire class into the Node heap and OOM-crashes
      // the process on large classes (e.g. Parse Dashboard pagination sends count=1 on
      // every page request).
      if (retval.type === 'count') {
        return findOperation
          .count()
          .then(result => result.count)
          .finally(async () => {
            if (localConn) {
              await localConn.close();
              localConn = null;
            }
          })
          .catch(error => {
            logger.error('Error running findOperation count, ERROR =' + error);
            throw error;
          });
      }

      logger.verbose('findOperation = ' + JSON.stringify(findOperation));
      logger.verbose('about to getDocuments()');
      let localDocs;
      return findOperation
        .getDocuments()
        .then(docs => {
          if (retval.type === 'content') {
            localDocs = docs.map(i => i.getContent());
          }
          if (retval.type === 'sodadocs') {
            localDocs = docs;
          }
          if (retval.type === 'one') {
            // return docs, keys and version
            if (docs && docs.length == 1) {
              const one = new Object();
              one.content = docs[0].getContent();
              one.key = docs[0].key;
              one.version = docs[0].version;
              localDocs = one;
            } else {
              if (docs && docs.length == 0) {
                return {};
              } else {
                logger.error('rawFind ONE return type found multiple docs');
                throw 'rawFind ONE return type found multiple docs';
              }
            }
          }
          if (retval.type === 'all') {
            // return docs, keys and version
            if (docs) {
              const returndocs = new Array();
              for (var i = 0; i < docs.length; i++) {
                const all = new Object();
                all.content = docs[i].getContent();
                all.key = docs[i].key;
                all.version = docs[i].version;
                returndocs.push(all);
              }
              localDocs = returndocs;
            }
          }
          return localDocs;
        })
        .finally(async () => {
          if (localConn) {
            await localConn.close();
            localConn = null;
          }
        })
        .catch(error => {
          logger.error('Error running findOperation GetDocuments, ERROR =' + error);
          throw error;
        });
    } catch (error) {
      if (localConn) {
        await localConn.close();
        localConn = null;
      }
      logger.error('Error running _rawfind, ERROR =' + error);
      throw error;
    }
  }

  //CDB 17-11 fix

  async distinct(field, query) {
    // Stream documents in bounded batches instead of materializing the whole class:
    // fetching everything at once (the previous behavior) OOM-crashes the process on
    // large classes. Only the distinct values are accumulated in memory.
    const BATCH_SIZE = 500;
    const values = new Set();
    let localConn = null;
    try {
      localConn = await this.getCollectionConnection();
      // Snapshot the handle: this._oracleCollection is re-bound (and its connection closed)
      // by any concurrent operation on this collection — the loop must stay on OUR connection.
      const collection = this._oracleCollection;
      for (let skip = 0; ; skip += BATCH_SIZE) {
        const docs = await collection
          .find()
          .filter(query)
          .skip(skip)
          .limit(BATCH_SIZE)
          .getDocuments();
        for (const doc of docs) {
          const content = _.get(doc.getContent(), field);
          if (Array.isArray(content)) {
            content.forEach(value => values.add(value));
          } else {
            values.add(content);
          }
        }
        if (docs.length < BATCH_SIZE) break;
      }
      return [...values];
    } catch (error) {
      logger.error('Error running distinct, ERROR =' + error);
      throw error;
    } finally {
      if (localConn) {
        await localConn.close();
        localConn = null;
      }
    }
  }
  //CDB-END

  async updateOne(query, update) {
    logger.verbose('UpdateOne calling findOneandUpdate');
    return this.findOneAndUpdate(query, update, null);
  }

  /**
   * Update multiple documents matching a query.
   * This is the Oracle SODA equivalent of MongoDB's updateMany.
   * Since Oracle SODA doesn't have a native bulk update, we iterate over matching documents.
   *
   * @param {Object} query - The query to match documents
   * @param {Object} update - The update to apply
   * @param {any} transactionalSession - Optional transaction session (not fully supported)
   * @returns {Promise<Array>} - Array of updated documents
   */
  async updateMany(query, update, transactionalSession) {
    try {
      logger.verbose('in Collection updateMany query = ' + JSON.stringify(query));
      logger.verbose(
        'use transactionalSession to make linter happy ' + JSON.stringify(transactionalSession)
      );

      // Find all matching documents
      const results = await this._rawFind(query, { type: 'all' }).then(result => {
        return result;
      });

      if (!results || results.length === 0) {
        logger.verbose('updateMany: No documents found matching query');
        return [];
      }

      logger.verbose('updateMany: Found ' + results.length + ' documents to update');

      const updatedDocs = [];

      // One connection and ONE commit for the whole matched set (previously:
      // connection acquire + commit per document).
      const ctx = await this._writeContext(transactionalSession);
      try {
      // Process each document
      for (let i = 0; i < results.length; i++) {
        const result = results[i];
        let currentUpdate = JSON.parse(JSON.stringify(update)); // Deep clone to avoid mutation

        // Apply the same update transformation logic as findOneAndUpdate
        let updateObj;

        //************************************************************************************************/
        // Modify Update based on Mongo operators
        //
        // Look for $unset, Mongo's deleteField
        const newUpdate = new Object();
        const fieldNames = new Array();
        Object.keys(currentUpdate).forEach(item => {
          if (item === '$unset') {
            Object.keys(currentUpdate[item]).forEach(fieldItem => {
              fieldNames.push(fieldItem);
            });
          } else {
            if (item === '_updated_at') {
              newUpdate['updatedAt'] = currentUpdate[item];
            } else {
              newUpdate[item] = currentUpdate[item];
            }
          }
        });

        // If fieldNames > 0, we need to handle field deletion for this specific document
        let currentContent = result.content;
        if (fieldNames.length > 0) {
          fieldNames.forEach(fieldName => {
            delete currentContent[fieldName];
          });
          currentUpdate = newUpdate;
        }

        // Process Increments $inc
        const newIncUpdate = new Object();
        let incUpdt = false;
        Object.keys(currentUpdate).forEach(item => {
          if (item === '$inc') {
            Object.keys(currentUpdate[item]).forEach(it2 => {
              incUpdt = true;
              _.set(currentContent, it2, _.result(currentContent, it2) + currentUpdate[item][it2]);
            });
          } else {
            if (item === '_updated_at') {
              newIncUpdate['updatedAt'] = currentUpdate[item];
            } else {
              newIncUpdate[item] = currentUpdate[item];
            }
          }
        });

        if (incUpdt) {
          currentUpdate = newIncUpdate;
        }

        // Process $addToSet
        const newAddToSetUpdate = new Object();
        let addToSetUpdt = false;
        Object.keys(currentUpdate).forEach(item => {
          if (item === '$addToSet') {
            Object.keys(currentUpdate[item]).forEach(it2 => {
              Object.keys(currentUpdate[item][it2]).forEach(it3 => {
                if (it3 === '$each') {
                  const updtArray = currentUpdate[item][it2][it3];
                  const temp = it2.split('.');
                  let newArray;
                  if (temp.length > 1) {
                    newArray = currentContent[temp[0]][temp[1]];
                  } else {
                    newArray = currentContent[it2];
                  }
                  if (newArray) {
                    updtArray.forEach(updt => {
                      if (typeof updt === 'object') {
                        if (!newArray.some(entry => Object.keys(entry)[0] === Object.keys(updt)[0])) {
                          addToSetUpdt = true;
                          newArray.push(updt);
                        }
                      } else {
                        if (!newArray.includes(updt)) {
                          addToSetUpdt = true;
                          newArray.push(updt);
                        }
                      }
                    });
                  }
                }
              });
            });
          } else {
            if (item === '_updated_at') {
              newAddToSetUpdate['updatedAt'] = currentUpdate[item];
            } else {
              newAddToSetUpdate[item] = currentUpdate[item];
            }
          }
        });

        if (addToSetUpdt) {
          currentUpdate = newAddToSetUpdate;
        }

        // Process $pullAll
        const newPullAllUpdate = new Object();
        let pullAllUpdt = false;
        Object.keys(currentUpdate).forEach(item => {
          if (item === '$pullAll') {
            Object.keys(currentUpdate[item]).forEach(it2 => {
              const updtArray = currentUpdate[item][it2];
              const rsltArray = currentContent[it2];
              if (rsltArray) {
                const newArray = new Array();
                updtArray.forEach(updt => {
                  if (typeof updt === 'object') {
                    rsltArray.forEach(entry => {
                      if (Object.keys(entry)[0] != Object.keys(updt)[0]) {
                        newArray.push(entry);
                        pullAllUpdt = true;
                      }
                    });
                  }
                  newPullAllUpdate[it2] = newArray;
                });
              }
            });
          } else {
            if (item === '_updated_at') {
              newPullAllUpdate['updatedAt'] = currentUpdate[item];
            } else {
              newPullAllUpdate[item] = currentUpdate[item];
            }
          }
        });

        if (pullAllUpdt) {
          currentUpdate = newPullAllUpdate;
        }

        // End of Transform Update
        //************************************************************************************************/

        const key = result.key;
        const version = result.version;
        const oldContent = currentContent;

        // (See findOneAndUpdate for the rationale.) An empty `{}` value in
        // `update` must NOT silently unset the field; merge handles it as
        // a no-op, which matches Mongo. Genuine clears go through $unset.
        if (currentUpdate.fieldName) {
          const theUpdate = { [currentUpdate.fieldName]: currentUpdate.theFieldType };
          updateObj = { ...oldContent, ...theUpdate };
        } else {
          if (pullAllUpdt || currentUpdate['_metadata']) {
            Object.keys(currentUpdate).forEach(item => {
              const found = Object.keys(oldContent).find(k => k === '_metadata');
              if (item === '_metadata') {
                if (found) {
                  if (
                    Object.prototype.hasOwnProperty.call(oldContent[item], 'class_permissions') &&
                    Object.prototype.hasOwnProperty.call(currentUpdate[item], 'class_permissions')
                  ) {
                    _.set(oldContent[item], 'class_permissions', currentUpdate[item]['class_permissions']);
                  } else {
                    _.merge(oldContent['_metadata'], currentUpdate[item]);
                  }
                } else {
                  _.set(oldContent, item, currentUpdate[item]);
                }
              } else {
                _.set(oldContent, item, currentUpdate[item]);
              }
            });
            updateObj = oldContent;
          } else {
            // See findOneAndUpdate above: array values in `currentUpdate`
            // must fully replace arrays in `oldContent` rather than merge
            // by index, otherwise shrinking arrays silently retain their
            // trailing elements.
            updateObj = _.mergeWith(oldContent, currentUpdate, (objVal, srcVal) =>
              Array.isArray(srcVal) ? srcVal : undefined
            );
          }
        }

        // Update the document
        const replaceResult = await ctx.collection
          .find()
          .key(key)
          .version(version)
          .replaceOne(updateObj);

        if (replaceResult.replaced === true) {
          updatedDocs.push(updateObj);
        } else {
          // Retry once if version mismatch.
          // Re-read on OUR connection: _rawFind would take a SECOND connection
          // from the pool while this one still holds uncommitted DML. Under
          // load every request ends up holding one connection and waiting for
          // another that can never come, so the pool deadlocks until
          // queueTimeout fires (NJS-040) -- and the row locks are held for that
          // whole wait.
          logger.verbose('updateMany: Retrying update for key ' + key);
          const retryDocs = await ctx.collection
            .find()
            .filter({ _id: oldContent._id })
            .getDocuments();
          if (retryDocs && retryDocs.length === 1) {
            const retryReplaceResult = await ctx.collection
              .find()
              .key(retryDocs[0].key)
              .version(retryDocs[0].version)
              .replaceOne(updateObj);
            if (retryReplaceResult.replaced === true) {
              updatedDocs.push(updateObj);
            }
          }
        }
      }

      await this._commitWrite(ctx);
      logger.verbose('updateMany: Successfully updated ' + updatedDocs.length + ' documents');
      return updatedDocs;
      } finally {
        await this._closeWriteContext(ctx);
      }
    } catch (error) {
      logger.error('Collection updateMany ERROR: ', error);
      throw error;
    }
  }

  async insertOne(object, transactionalSession) {
    // Note: the previous version ran an ALTER SESSION ddl_lock_timeout PL/SQL
    // block before every insert — that is only needed for DDL (createIndex),
    // not DML, and cost one extra round trip per row.
    const ctx = await this._writeContext(transactionalSession);
    try {
      const result = await ctx.collection.insertOne(object);
      await this._commitWrite(ctx);
      return result;
    } catch (error) {
      logger.error('error during insertOne = ' + error);
      throw error;
    } finally {
      await this._closeWriteContext(ctx);
    }
  }

  async drop() {
    let localConn = null;

    logger.verbose('entered drop for ' + this._name);
    return this.getCollectionConnection()
      .then(conn => {
        localConn = conn;
        return this._oracleCollection.drop();
      })
      .then(result => {
        if (result) {
          logger.verbose('drop succeeded for  ' + this._name);
        } else {
          logger.verbose('drop failed for  ' + this._name);
        }
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('in Drop Error' + error);
        throw error;
      });
  }

  async truncate() {
    // collection.truncate() does not work with instant clients less than version 20
    // https://oracle.github.io/node-oracledb/doc/api.html#-11212-sodacollectiontruncate
    // Error: DPI-1050: Oracle Client library is at version 19.8 but version 20.1 or higher is needed
    // for now, do it the old fashioned way with collection.find.remove
    let localConn = null;
    return this.getCollectionConnection()
      .then(async conn => {
        localConn = conn;
        const result = await this._oracleCollection.find().remove();
        await localConn.commit();
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('in truncate Error' + error);
        throw error;
      });
  }
  async _fetchAllSchemasFrom_SCHEMA() {
    return this._rawFind({}, { type: 'content' })
      .then(schemas => {
        logger.verbose('schemas = ' + schemas);
        return schemas;
      })
      .catch(error => {
        logger.error('error during fetchAllSchemasFrom_SCHEMA = ' + error);
        throw error;
      });
  }

  getCollectionName() {
    return this._name;
  }

  _ensureSparseUniqueIndexInBackground(indexRequest) {
    // TODO rewrite params to suit oracle soda
    logger.verbose(
      'entered _ensureSparseUniqueIndexInBackground with indexRequest = ' +
        JSON.stringify(indexRequest)
    );
    return this._createIndex(indexRequest);
  }

  async _createIndex(indexSpec) {
    let localConn = null;

    logger.verbose('_createIndex index spec is ' + JSON.stringify(indexSpec));
    return await this.getCollectionConnection()
      .then(async conn => {
        localConn = conn;
        await localConn.execute(ddlTimeOut);
        await this._oracleCollection.createIndex(indexSpec);
        return Promise.resolve;
      })
      .then(result => {
        // Parse expects _id index in Schema to be
        // _metadata: { indexes: { _id_: { _id: 1 }, name_1: { name: 1 } } }
        const idx = {};
        indexSpec.fields.forEach(field => {
          idx[field.path] = 1;
        });
        if (indexSpec.fields[0].path === '_id') {
          indexSpec.name = '_id_';
        }
        const obj = { [indexSpec.name]: idx };
        this.indexes.push(obj);
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        if (error.errorNum === 40733) {
          /*
          Rebuild internal indexes array on server restart from schema indexes
          */
          const found = this.indexes.find(item => {
            // Parse expects _id index in Schema to be
            // _metadata: { indexes: { _id_: { _id: 1 }, name_1: { name: 1 } } }
            if (indexSpec.fields[0].path === '_id') {
              indexSpec.fields[0].path = '_id_';
            }
            return Object.keys(item)[0] === indexSpec.fields[0].path;
          });

          if (typeof found === 'undefined') {
            const idx = {};
            indexSpec.fields.forEach(field => {
              idx[field.path] = 1;
            });
            if (indexSpec.fields[0].path === '_id') {
              indexSpec.name = '_id_';
            }
            const obj = { [indexSpec.name]: idx };
            this.indexes.push(obj);
            return Promise.resolve;
          }
          logger.verbose('Index' + JSON.stringify(indexSpec) + ' already exists');
        } else {
          logger.error('createIndex throws ' + error);
          throw error;
        }
      });
  }

  // Extracts the JSON path from a function-based index expression, e.g.
  //   JSON_VALUE("JSON_DOCUMENT" FORMAT OSON , '$."_p_conversation"' RETURNING ...)  -> _p_conversation
  //   json_value("JSON_DOCUMENT",'$."a"."b"' returning ...)                          -> a.b
  _jsonPathFromIndexExpression(expression) {
    if (!expression) {
      return null;
    }
    const match = /'(\$[^']*)'/.exec(expression);
    if (!match) {
      return null;
    }
    const path = match[1].replace(/"/g, '').replace(/^\$\.?/, '');
    return path.length > 0 ? path : null;
  }

  // Reads the collection's actual indexes from the data dictionary and returns
  // them in the format Parse stores in _SCHEMA:  { indexName: { field: 1, ... } }
  // The unique _id index (named 'ididx<collection>') is reported as '_id_'.
  async getIndexes(className) {
    logger.verbose('OracleCollection getIndexes className = ' + className);
    let localConn = null;
    try {
      const pool = await this._oracleStorageAdapter.connect();
      localConn = await pool.getConnection();
      const result = await localConn.execute(
        `SELECT i.index_name AS index_name,
                e.column_expression AS column_expression,
                e.column_position AS column_position
           FROM user_indexes i
           JOIN user_ind_expressions e
             ON e.index_name = i.index_name
            AND e.table_name = i.table_name
          WHERE i.table_name = :tableName
          ORDER BY i.index_name, e.column_position`,
        { tableName: this._name },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const indexes = {};
      for (const row of result.rows) {
        const path = this._jsonPathFromIndexExpression(row.COLUMN_EXPRESSION);
        if (!path) {
          continue;
        }
        const name = row.INDEX_NAME === 'ididx' + this._name || path === '_id' ? '_id_' : row.INDEX_NAME;
        if (!indexes[name]) {
          indexes[name] = {};
        }
        // Mirror the datatype convention used on index creation: non-varchar2
        // functional indexes are reported as their datatype string so a
        // schema target like {createdAt: 'timestamp'} round-trips cleanly.
        const returning = /RETURNING\s+(TIMESTAMP|NUMBER|DATE)/i.exec(row.COLUMN_EXPRESSION);
        indexes[name][path] = returning ? returning[1].toLowerCase() : 1;
      }
      if (!indexes['_id_']) {
        indexes['_id_'] = { _id: 1 };
      }
      logger.verbose('getIndexes returns ' + JSON.stringify(indexes));
      return indexes;
    } catch (error) {
      logger.error('getIndexes dictionary lookup failed for ' + this._name + ': ' + error);
      // Fall back to the legacy in-memory bookkeeping (only knows indexes
      // created during this process lifetime), normalized to Parse format.
      const legacy = { _id_: { _id: 1 } };
      this.indexes.forEach(item => {
        Object.keys(item).forEach(name => {
          const spec = item[name];
          legacy[name] = Array.isArray(spec) ? Object.assign({}, ...spec) : spec;
        });
      });
      return legacy;
    } finally {
      if (localConn) {
        await localConn.close();
        localConn = null;
      }
    }
  }

  async dropIndex(indexName) {
    logger.verbose('Collection ' + this._name + ' is dropping index' + indexName);
    let localConn = null;

    const result = await this.getCollectionConnection()
      .then(async conn => {
        localConn = conn;
        const result = await this._oracleCollection.dropIndex(indexName);
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('error during dropIndex = ' + error);
        throw error;
      });

    const found = this.indexes.find(item => {
      return Object.keys(item)[0] === indexName;
    });
    if (found) {
      this.indexes.splice(this.indexes.indexOf(found), 1);
    }

    return result;
  }
}
