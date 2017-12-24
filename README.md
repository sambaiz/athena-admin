# athena-admin

Migrate the table schema, rename object prefix to partition key=value and add partitions.

```
$ npm install athena-admin
```

```
const AthenaAdmin = require('athena-admin').AthenaAdmin;
const dbDef = require('./sampledatabase.json');
const admin = new AthenaAdmin(dbDef);
await admin.replaceObjects();
await admin.migrate();
await admin.partition();
```

## Database definition

Describe the database definition in the following format.

```
{
  "general": {
    "athenaRegion": "ap-northeast-1",
    "databaseName": "aaaa",
    "saveDefinitionLocation": "s3://saveDefinitionBucket/aaaa.json"
  },
  "tables": {
    "sample_data": {
      "columns": {
        "user_id": "int",
        "value": "struct<score:int,category:string>"
      },
      "srcLocation": "s3://src/location/",
      "partition": {
        "prePartitionLocation": "s3://pre/partition/", /* optional */
        "regexp": "(\\d{4})/(\\d{2})/(\\d{2})/", /* optional */
        "keys": [
          {
            "name": "dt",
            "type": "string",
            "format": "{1}-{2}-{3}", /* optional */
          }
        ]
      }
    }
  }
}
```

### general

| Field  | Description |
|:-----------|:------------|
| athenaRegion | Region for Athena |
| databaseName | Athena database name |
| saveDefinitionLocation | Location to save the previous definition |

### tables

- Root field name (sample_data) is a table name.

| Field  | Description |
|:-----------|:------------|
| columns | Column name and type pairs |
| srcLocation | Location to be refferenced by Athena |
| partition | Partition detectable by key=value prefix.<br>If objects' location don't have partition's key=value prefix, you can replace from prePartitionLocation to srcLocation by `replaceObjects()`. This is for `partition()` automatically detecting and adding partitions with keys.key as its key and keys.format as its value of keys.type as its type.<br>keys.format's {n} corresponds to the group of regexp. (e.g. `s3://pre/partition/2017/12/01/00/aaa.png` => `[2017/12/01, 2017, 12, 01]`) |

### API

### replaceObjects(deletePreObject=true, matchedHandler=(matched, objKey, table)=>matched)

Replaces object located in prePartitionLocation to srcLocation with partition key=value prefix.
(e.g. `s3://pre/partition/2017/12/01/00/aaa.png` => `s3://src/location/dt=2017-12-01/00/aaa.png`)

If you need to change the key before this operation, use matchedHandler.
The following example is changing the UTC string to that of TimeZone.
(e.g. `2017/12/01/19` => `2017/12/02/04`)
There is full codes in /sample.

```
const utcToTZ = (matched, objKey, table) => {
  let existsDt = false;
  table.partition.keys.forEach((key) => {
    if (key.name === 'dt') {
      existsDt = true;
    }
  });
  if (!existsDt) {
    return matched;
  }

  let tz = moment(`${matched[0]} +00:00`, 'YYYY/MM/DD/HH ZZ');
  matched[1] = tz.format('YYYY');
  matched[2] = tz.format('MM');
  matched[3] = tz.format('DD');
  matched[4] = tz.format('HH');
  return matched;
};

await admin.replaceObjects(false, utcToTZ);
```

### migrate()

If there is differences from the previous saved definition in S3, create/drop the table or update the schema.

### partition()

Just run `MSCK REPAIR TABLE`. Partition is automatically detected and added by objects' key=value prefix.