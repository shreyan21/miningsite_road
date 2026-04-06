import { MINING_SITE_FILTER, PLANNER_METADATA, SCHOOL_SOURCES, TABLES } from '../config/datasets.js';
import { qualifiedTable, quoteIdentifier, quoteLiteral, tableExists } from '../utils/sql.js';

const buildMiningSiteWhereClause = (tableAlias = null) => {
  const columnPrefix = tableAlias ? `${tableAlias}.` : '';
  const values = MINING_SITE_FILTER.includedValues.map((value) => quoteLiteral(value)).join(', ');
  return `${columnPrefix}${quoteIdentifier(MINING_SITE_FILTER.nameColumn)} IN (${values})`;
};

const featureCollectionFromRows = (rows) => ({
  type: 'FeatureCollection',
  features: rows.map((row) => ({
    type: 'Feature',
    id: row.id ?? undefined,
    geometry: row.geometry,
    properties: row.properties || {},
  })),
});

const loadGeoJsonRows = async (pool, sql, params = []) => {
  const result = await pool.query(sql, params);
  return featureCollectionFromRows(result.rows);
};

const getExistingSchoolSources = async (pool) => {
  const entries = [];
  for (const source of SCHOOL_SOURCES) {
    if (await tableExists(pool, source.tableName)) {
      entries.push(source);
    }
  }
  return entries;
};

const getSchoolDisplaySources = (sources) =>
  sources.filter((source) => !source.isBuffered && source.useForDisplay !== false);

const getSchoolObstacleSources = (sources) => {
  const bufferedSources = sources.filter((source) => source.isBuffered && source.useForObstacles !== false);
  if (bufferedSources.length > 0) {
    return [
      ...bufferedSources,
      ...sources.filter((source) => !source.isBuffered && source.tableName === 'gorakhpur_ps' && source.useForObstacles !== false),
    ];
  }

  return sources.filter((source) => source.useForObstacles !== false);
};

export const getMiningSitesGeoJson = async (pool) => {
  const sql = `
    SELECT
      gb.gid AS id,
      ST_AsGeoJSON(ST_Transform(gb.geom, 4326))::jsonb AS geometry,
      jsonb_build_object(
        'name', gb.name,
        'district', gb.district,
        'is_connected', COALESCE(mcs.is_connected, false),
        'connection_cost', mcs.connection_cost,
        'path_length', mcs.path_length,
        'reason_code', mcs.reason_code,
        'path_strategy', mcs.path_strategy
      ) AS properties
    FROM gorakhpur_brickkiln gb
    LEFT JOIN mining_connection_status mcs ON mcs.mining_gid = gb.gid
    WHERE gb.geom IS NOT NULL
      AND ${buildMiningSiteWhereClause('gb')}
    ORDER BY gb.gid
  `;
  return loadGeoJsonRows(pool, sql);
};

export const getRoadNetworkGeoJson = async (pool) => {
  const sql = `
    SELECT
      gid AS id,
      ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb AS geometry,
      jsonb_build_object(
        'road_type', road_type,
        'length_km', length_km,
        'cost', cost,
        'source_mining_site', source_mining_site,
        'is_bypass', is_bypass
      ) AS properties
    FROM road_network
    WHERE geom IS NOT NULL
      AND (
        source_mining_site IS NOT NULL
        OR road_type IN ('national_highway', 'state_highway', 'highway', 'expressway')
        OR source_table ILIKE '%highw%'
        OR source_table ILIKE '%exp%'
      )
    ORDER BY gid
  `;
  return loadGeoJsonRows(pool, sql);
};

export const getRiversGeoJson = async (pool) => {
  const sql = `
    SELECT
      gid AS id,
      ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb AS geometry,
      jsonb_build_object('name', wetname, 'area', area) AS properties
    FROM uprsac_09xxxx_riverxxxxx_09042018
    WHERE geom IS NOT NULL
    ORDER BY gid
  `;
  return loadGeoJsonRows(pool, sql);
};

export const getSchoolsGeoJson = async (pool) => {
  const schoolSources = getSchoolDisplaySources(await getExistingSchoolSources(pool));
  if (schoolSources.length === 0) {
    return featureCollectionFromRows([]);
  }

  const unions = schoolSources.map((source) => `
    SELECT
      ${quoteLiteral(`${source.tableName}:`)} || src.${quoteIdentifier(source.idColumn || 'gid')}::text AS id,
      ST_AsGeoJSON(
        ST_Transform(
          ST_PointOnSurface(src.${quoteIdentifier(source.geomColumn)}),
          4326
        )
      )::jsonb AS geometry,
      jsonb_build_object(
        'name', ${
          source.nameColumn
            ? `COALESCE(src.${quoteIdentifier(source.nameColumn)}, ${quoteLiteral(source.label || 'Unnamed School')})`
            : quoteLiteral(source.label || 'Unnamed School')
        },
        'district', ${source.districtColumn ? `COALESCE(src.${quoteIdentifier(source.districtColumn)}, '')` : "''"},
        'source_id', src.${quoteIdentifier(source.idColumn || 'gid')},
        'source', ${quoteLiteral(source.tableName)},
        'label', ${quoteLiteral(source.label)},
        'is_buffer', ${source.isBuffered ? 'true' : 'false'},
        'display_geometry', ${source.isBuffered ? quoteLiteral('point_on_buffer') : quoteLiteral('source_geometry')}
      ) AS properties
    FROM ${qualifiedTable(source.tableName)} src
    WHERE src.${quoteIdentifier(source.geomColumn)} IS NOT NULL
  `);

  return loadGeoJsonRows(pool, unions.join(' UNION ALL '));
};

export const getObstacleGeoJson = async (pool, { schoolBuffer, includeSchoolBuffers = true, includeRivers = true, includeMiningSites = true }) => {
  const schoolSources = getSchoolObstacleSources(await getExistingSchoolSources(pool));
  const parts = [];
  const params = [schoolBuffer];

  if (includeSchoolBuffers) {
    for (const source of schoolSources) {
      parts.push(`
        SELECT
          ${quoteLiteral(`${source.tableName}:`)} || ${quoteIdentifier(source.idColumn || 'gid')}::text AS id,
          ST_AsGeoJSON(ST_Transform(${
            source.isBuffered
              ? quoteIdentifier(source.geomColumn)
              : `ST_Buffer(${quoteIdentifier(source.geomColumn)}, $1)`
          }, 4326))::jsonb AS geometry,
          jsonb_build_object(
            'type', 'school_buffer',
            'name', ${
              source.nameColumn
                ? `COALESCE(${quoteIdentifier(source.nameColumn)}, ${quoteLiteral(source.label || 'Unnamed School')})`
                : quoteLiteral(source.label || 'Unnamed School')
            },
            'source', ${quoteLiteral(source.tableName)},
            'is_buffer', true,
            'buffer_distance', ${source.isBuffered ? '0' : '$1'},
            'source_was_buffered', ${source.isBuffered ? 'true' : 'false'}
          ) AS properties
        FROM ${qualifiedTable(source.tableName)}
        WHERE ${quoteIdentifier(source.geomColumn)} IS NOT NULL
      `);
    }
  }

  if (includeRivers && await tableExists(pool, TABLES.rivers)) {
    parts.push(`
      SELECT
        'river:' || gid::text AS id,
        ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb AS geometry,
        jsonb_build_object('type', 'river', 'name', wetname) AS properties
      FROM ${qualifiedTable(TABLES.rivers)}
      WHERE geom IS NOT NULL
    `);
  }

  if (includeMiningSites) {
    parts.push(`
      SELECT
        'mining_site:' || gid::text AS id,
        ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb AS geometry,
        jsonb_build_object('type', 'mining_site', 'name', COALESCE(name, 'Mining Site')) AS properties
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE geom IS NOT NULL
        AND ${buildMiningSiteWhereClause()}
    `);
  }

  if (parts.length === 0) {
    return featureCollectionFromRows([]);
  }

  return loadGeoJsonRows(pool, parts.join(' UNION ALL '), params);
};

export const getRoadSourceSummary = async (pool) => {
  const result = await pool.query(`
    SELECT
      rsr.id,
      rsr.table_name,
      rsr.road_type,
      rsr.enabled,
      COUNT(rn.gid)::int AS seeded_segments
    FROM road_source_registry rsr
    LEFT JOIN road_network rn
      ON rn.source_table = rsr.table_name
      AND rn.source_mining_site IS NULL
    GROUP BY rsr.id
    ORDER BY rsr.id
  `);

  return result.rows;
};

export const getMapLayers = async (
  pool,
  { schoolBuffer, includeSchools = false, includeObstacles = false, includeRoadSources = false },
) => {
  const [roads, rivers, miningSites, schools, obstacles, roadSources] = await Promise.all([
    getRoadNetworkGeoJson(pool),
    getRiversGeoJson(pool),
    getMiningSitesGeoJson(pool),
    includeSchools ? getSchoolsGeoJson(pool) : Promise.resolve(featureCollectionFromRows([])),
    includeObstacles
      ? getObstacleGeoJson(pool, { schoolBuffer })
      : Promise.resolve(featureCollectionFromRows([])),
    includeRoadSources ? getRoadSourceSummary(pool) : Promise.resolve([]),
  ]);

  return {
    planner: PLANNER_METADATA,
    schoolBuffer,
    roadSources,
    layers: {
      roads,
      rivers,
      schools,
      miningSites,
      obstacles,
    },
  };
};
