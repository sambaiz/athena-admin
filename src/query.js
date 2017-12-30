export const dropTableIfExistsQuery = (databaseName, tableName) => {
  return `DROP TABLE IF EXISTS ${databaseName}.${tableName}`;
};

export const createTableQuery = (databaseName, tableName, columns, partitionKeys, location) => {
  const columnsStr = Object.keys(columns)
    .map((c) => `${c} ${buildTypeName(columns[c])}`)
    .join(',');
  const partitionsStr = partitionKeys.map((key) => `${key.name} ${key.type}`).join(',');
  return `
  CREATE EXTERNAL TABLE ${databaseName}.${tableName} (${columnsStr}) 
  PARTITIONED BY(${partitionsStr}) 
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION '${location}';
  `;
};

const buildTypeName = (typeDef) => {
  if (typeof typeDef !== 'object') {
    return typeDef;
  }
  if (Array.isArray(typeDef)) {
    return `array<${buildTypeName(typeDef[0])}>`;
  }
  const structContents = Object.keys(typeDef).map((t) => {
    return `${t}:${buildTypeName(typeDef[t])}`;
  }).join(',');
  return `struct<${structContents}>`;
};
