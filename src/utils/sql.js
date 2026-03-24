const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

export const quoteIdentifier = (identifier) => {
  if (!IDENTIFIER_PATTERN.test(identifier)) {
    throw new Error(`Unsafe SQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
};

export const qualifiedTable = (tableName, schema = 'public') =>
  `${quoteIdentifier(schema)}.${quoteIdentifier(tableName)}`;

export const quoteLiteral = (value) => `'${String(value).replace(/'/g, "''")}'`;

export const tableExists = async (pool, tableName, schema = 'public') => {
  const result = await pool.query(
    `SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = $1 AND table_name = $2
    ) AS exists`,
    [schema, tableName],
  );
  return result.rows[0].exists;
};

export const columnExists = async (pool, tableName, columnName, schema = 'public') => {
  const result = await pool.query(
    `SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
    ) AS exists`,
    [schema, tableName, columnName],
  );
  return result.rows[0].exists;
};

export const ensureNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};
