export const dropTableIfExistsQuery = (databaseName, tableName) => {
  return `DROP TABLE IF EXISTS ${databaseName}.${tableName}`;
};

export const createTableQuery = (databaseName, tableName, columns, partitionKeys, location) => {
  const columnsStr = Object.keys(columns)
    .map((c) => `${c} ${columns[c]}`)
    .join(',');
  const partitionsStr = partitionKeys.map((key) => `${key.name} ${key.type}`).join(',');
  return `
  CREATE EXTERNAL TABLE ${databaseName}.${tableName} (${columnsStr}) 
  PARTITIONED BY(${partitionsStr}) 
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION '${location}';
  `;
};
