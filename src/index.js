import {listObject, getObject, copyObject, putObject, deleteObject, splitS3Location} from './s3';
import {dropTableIfExistsQuery, createTableQuery} from './query';
import {log} from './log';
import {Client as athenaClient} from 'athena-client';

/**
 * Migrate the table schema,
 * rename object prefix to partition key=value
 * and add partitions.
 */
export class AthenaAdmin {
  /**
   * constructor
   * @param {Object} dbDef
   */
  constructor(dbDef) {
    this.dbDef = dbDef;
  }

  /**
   * Replaces object located in prePartitionLocation to srcLocation
   * with partition key=value prefix.
   * @param {boolean} deletePreObject
   * @param {Function} matchedHandler
   */
  async replaceObjects(deletePreObject = true, matchedHandler = (matched, objKey, table) => matched) {
    const tables = this.dbDef.tables;
    // eslint-disable-next-line guard-for-in
    for (let tableName in tables) {
      if (
        !tables[tableName].partition.prePartitionLocation ||
        !tables[tableName].partition.regexp
      ) {
        log(`[replaceObjects] skip the table ${tableName}`);
        continue;
      }
      const preLocation = tables[tableName].partition.prePartitionLocation;
      const {bucket: preBucket, path: prePath} = splitS3Location(preLocation);
      await Promise.all(
        (await listObject(preBucket, prePath)).Contents.map(async (object) => {
          if (object.Key.endsWith('/')) {
            return Promise.resolve(); // skip directory
          }
          const re = new RegExp(`^${prePath}${tables[tableName].partition.regexp}`);
          const matched = matchedHandler(object.Key.match(re), object.Key, tables[tableName]);
          const replacedKey = object.Key.replace(
            matched[0],
            tables[tableName].partition.keys
              .map((key) => {
                for (let i = 1; i < matched.length; i++) {
                  key.format = key.format.replace(new RegExp(`\\{${i}\\}`), matched[i]);
                }
                if (key.type === 'int') {
                  // e.g. 01 => 1
                  key.format = Number(key.format);
                }
                return `${key.name}=${key.format}`;
              })
              .join('/')
          );

          const newLocation = `${tables[tableName].srcLocation}${replacedKey}`;
          const {bucket, path} = splitS3Location(newLocation);
          await copyObject(bucket, `${preBucket}/${object.Key}`, path);
          log(`[replaceObjects] ${object.Key} -> ${path}`);
          if (deletePreObject) {
            await deleteObject(preBucket, object.Key);
          }
        })
      );
    }
  }

  /**
   * If there is differences from the previous saved definition,
   * create/drop the table or update the schema.
   */
  async migrate() {
    const {bucket, path} = splitS3Location(this.dbDef.general.saveDefinitionLocation);
    let previousDef = await getObject(bucket, path).catch((err) => {
      if (err.statusCode === 404) {
        log('[migrate] previous definition file is not found.');
        return Promise.resolve({});
      } else {
        return Promise.reject(err);
      }
    });
    if (previousDef.Body) {
      previousDef = JSON.parse(previousDef.Body.toString());
    }
    let dropTableNames = [];
    let createTableNames = [];
    const tables = this.dbDef.tables;
    // eslint-disable-next-line guard-for-in
    for (let tableName in tables) {
      if (
        !previousDef.tables ||
        !previousDef.tables[tableName] || // new
        JSON.stringify(previousDef.tables[tableName]) !== JSON.stringify(tables[tableName]) // update
      ) {
        createTableNames.push(tableName);
      }
      if (previousDef.tables) {
        delete previousDef.tables[tableName];
      }
    }

    if (previousDef.tables) {
      dropTableNames = dropTableNames.concat(Object.keys(previousDef.tables));
    }

    if (createTableNames.length > 0) {
      log(`[migrate] new/update table: ${createTableNames.join(',')}`);
    }

    if (dropTableNames.length > 0) {
      log(`[migrate] drop table: ${dropTableNames.join(',')}`);
    }

    if (previousDef.general) {
      await Promise.all(
        dropTableNames.map((name) => {
          return this._newAthenaClient(previousDef, name).execute(
            dropTableIfExistsQuery(previousDef.general.databaseName, name)
          );
        })
      );
    }
    await Promise.all(
      createTableNames.map(async (name) => {
        const table = tables[name];
        const client = this._newAthenaClient(this.dbDef, name);
        await client.execute(dropTableIfExistsQuery(this.dbDef.general.databaseName, name));
        return client.execute(
          createTableQuery(
            this.dbDef.general.databaseName,
            name,
            table.columns,
            table.partition.keys,
            table.srcLocation
          )
        );
      })
    );
    await putObject(bucket, path, JSON.stringify(this.dbDef));
  }

  /**
   * Just run `MSCK REPAIR TABLE`.
   * Partition is automatically detected and added by objects' key=value prefix.
   */
  async partition() {
    await Promise.all(
      Object.keys(this.dbDef.tables).map((tableName) => {
        return this._newAthenaClient(this.dbDef, tableName).execute(
          `MSCK REPAIR TABLE ${this.dbDef.general.databaseName}.${tableName}`
        );
      })
    );
  }

  /**
   * new Athena client
   * @param {Object} dbDef
   * @param {string} tableName
   * @return {Object}
   */
  _newAthenaClient(dbDef, tableName) {
    const {bucket} = splitS3Location(dbDef.tables[tableName].srcLocation);
    return athenaClient(
      {
        region: dbDef.general.athenaRegion,
      },
      {
        bucketUri: `s3://${bucket}/tmp/`,
      }
    );
  }
}
