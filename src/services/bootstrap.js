import { DEFAULT_ROAD_SOURCES, TABLES } from '../config/datasets.js';
import { columnExists, qualifiedTable, quoteIdentifier, tableExists } from '../utils/sql.js';

const SUPPORT_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS road_source_registry (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    road_type VARCHAR(50) NOT NULL,
    geom_column TEXT NOT NULL DEFAULT 'geom',
    length_column TEXT,
    road_code_column TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS road_network (
    gid SERIAL PRIMARY KEY,
    road_type VARCHAR(50) NOT NULL,
    source_table TEXT,
    source_gid INTEGER,
    source_mining_site INTEGER,
    target_mining_site INTEGER,
    length_km NUMERIC,
    cost DOUBLE PRECISION,
    reverse_cost DOUBLE PRECISION,
    source BIGINT,
    target BIGINT,
    geom geometry(LineString, 32644) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_curved BOOLEAN NOT NULL DEFAULT false,
    is_bypass BOOLEAN NOT NULL DEFAULT false,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
  );

  CREATE TABLE IF NOT EXISTS mining_connection_status (
    mining_gid INTEGER PRIMARY KEY REFERENCES gorakhpur_brickkiln(gid) ON DELETE CASCADE,
    is_connected BOOLEAN NOT NULL DEFAULT false,
    connection_road_gid INTEGER REFERENCES road_network(gid) ON DELETE SET NULL,
    connection_cost NUMERIC,
    path_length NUMERIC,
    path_strategy TEXT,
    reason_code TEXT,
    reason_detail TEXT,
    connected_at TIMESTAMP WITHOUT TIME ZONE,
    entry_point_geom geometry(Point, 32644),
    connection_point_geom geometry(Point, 32644),
    route_geom geometry(LineString, 32644),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
  );

  CREATE INDEX IF NOT EXISTS idx_road_network_geom ON road_network USING GIST (geom);
  CREATE INDEX IF NOT EXISTS idx_road_network_type ON road_network (road_type);
  CREATE INDEX IF NOT EXISTS idx_road_network_source_mining ON road_network (source_mining_site);
  CREATE INDEX IF NOT EXISTS idx_mining_connection_connected ON mining_connection_status (is_connected);
`;

const ensureDatasetIndexes = async (pool) => {
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_gorakhpur_brickkiln_geom_runtime
    ON gorakhpur_brickkiln USING GIST (geom);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_river_geom_runtime
    ON uprsac_09xxxx_riverxxxxx_09042018 USING GIST (geom);
  `);

  if (await tableExists(pool, 'uprsac_09xxxx_educschool_20132016')) {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_school_geom_runtime
      ON uprsac_09xxxx_educschool_20132016 USING GIST (geom);
    `);
  }

  if (await tableExists(pool, 'gorakhpur_ps')) {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_gorakhpur_ps_geom_runtime
      ON gorakhpur_ps USING GIST (geom);
    `);
  }
};

const insertRoadSource = async (pool, source) => {
  if (!(await tableExists(pool, source.tableName))) {
    return;
  }

  const hasGeom = await columnExists(pool, source.tableName, source.geomColumn);
  if (!hasGeom) {
    throw new Error(`Road source ${source.tableName} is missing geometry column ${source.geomColumn}.`);
  }

  await pool.query(
    `INSERT INTO road_source_registry (table_name, road_type, geom_column, length_column, road_code_column)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (table_name)
     DO UPDATE SET
       road_type = EXCLUDED.road_type,
       geom_column = EXCLUDED.geom_column,
       length_column = EXCLUDED.length_column,
       road_code_column = EXCLUDED.road_code_column`,
    [
      source.tableName,
      source.roadType,
      source.geomColumn,
      source.lengthColumn || null,
      source.roadCodeColumn || null,
    ],
  );
};

const insertRoadSourceRows = async (pool, source) => {
  const tableName = qualifiedTable(source.table_name);
  const geomColumn = quoteIdentifier(source.geom_column);
  const lengthColumn = source.length_column ? quoteIdentifier(source.length_column) : null;
  const codeColumn = source.road_code_column ? quoteIdentifier(source.road_code_column) : null;

  const sql = `
    INSERT INTO road_network (
      road_type,
      source_table,
      source_gid,
      length_km,
      cost,
      reverse_cost,
      geom,
      metadata
    )
    SELECT
      $1,
      $2,
      src.gid,
      ${lengthColumn ? `COALESCE(src.${lengthColumn}::numeric, ST_Length(d.geom) / 1000.0)` : 'ST_Length(d.geom) / 1000.0'},
      ${lengthColumn ? `COALESCE(src.${lengthColumn}::double precision, ST_Length(d.geom))` : 'ST_Length(d.geom)'},
      ${lengthColumn ? `COALESCE(src.${lengthColumn}::double precision, ST_Length(d.geom))` : 'ST_Length(d.geom)'},
      d.geom::geometry(LineString, 32644),
      jsonb_strip_nulls(
        jsonb_build_object(
          'road_code', ${codeColumn ? `src.${codeColumn}` : 'NULL'},
          'seeded_from_registry', true
        )
      )
    FROM ${tableName} src
    CROSS JOIN LATERAL (
      SELECT dump.geom::geometry(LineString, 32644) AS geom
      FROM ST_Dump(ST_CollectionExtract(ST_LineMerge(ST_ForceCollection(src.${geomColumn})), 2)) AS dump
    ) d
    WHERE src.${geomColumn} IS NOT NULL
  `;

  await pool.query(sql, [source.road_type, source.table_name]);
};

export const reseedBaseRoadNetwork = async (pool) => {
  const sourceResult = await pool.query(`
    SELECT table_name, road_type, geom_column, length_column, road_code_column
    FROM road_source_registry
    WHERE enabled = true
    ORDER BY id
  `);

  await pool.query('DELETE FROM mining_connection_status');
  await pool.query('DELETE FROM road_network WHERE source_mining_site IS NOT NULL');
  await pool.query('DELETE FROM road_network WHERE source_mining_site IS NULL');

  for (const source of sourceResult.rows) {
    if (!(await tableExists(pool, source.table_name))) {
      continue;
    }
    await insertRoadSourceRows(pool, source);
  }
};

export const bootstrapDatabase = async (pool) => {
  if (!(await tableExists(pool, TABLES.miningSites))) {
    throw new Error(`Mining site table ${TABLES.miningSites} is missing.`);
  }

  await pool.query(SUPPORT_TABLE_SQL);
  await ensureDatasetIndexes(pool);

  for (const source of DEFAULT_ROAD_SOURCES) {
    await insertRoadSource(pool, source);
  }

  const roadCountResult = await pool.query('SELECT COUNT(*)::int AS count FROM road_network');
  if (roadCountResult.rows[0].count === 0) {
    await reseedBaseRoadNetwork(pool);
  }
};
