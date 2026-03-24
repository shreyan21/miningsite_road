import { SCHOOL_SOURCES, TABLES } from '../config/datasets.js';
import { qualifiedTable, quoteIdentifier, tableExists } from '../utils/sql.js';
import { reseedBaseRoadNetwork } from './bootstrap.js';

const CANDIDATE_LIMIT = 10;
const MAX_DETOUR_DEPTH = 4;
const DETOUR_CLEARANCE = 30;
const PLANNER_MODE = 'obstacle_aware_polyline';
const TEMP_OBSTACLE_CACHE = 'temp_route_obstacle_cache';

const pointWkt = ([x, y]) => `POINT(${x} ${y})`;
const lineWkt = (points) => `LINESTRING(${points.map(([x, y]) => `${x} ${y}`).join(', ')})`;

const parsePointWkt = (wkt) => {
  const match = /^POINT\(([-\d.]+) ([-\d.]+)\)$/i.exec(wkt.trim());
  if (!match) throw new Error(`Unable to parse point WKT: ${wkt}`);
  return [Number(match[1]), Number(match[2])];
};

const parseLineStringWkt = (wkt) => {
  const match = /^LINESTRING\((.+)\)$/i.exec(wkt.trim());
  if (!match) throw new Error(`Unable to parse linestring WKT: ${wkt}`);
  return match[1].split(',').map((pair) => {
    const [x, y] = pair.trim().split(/\s+/).map(Number);
    return [x, y];
  });
};

const dedupeSequentialPoints = (points) => {
  const deduped = [];
  for (const point of points) {
    const prev = deduped[deduped.length - 1];
    if (!prev || prev[0] !== point[0] || prev[1] !== point[1]) {
      deduped.push(point);
    }
  }
  return deduped;
};

const replaceSegmentWithPath = (points, segmentIndex, replacementPoints) => {
  const prefix = points.slice(0, segmentIndex);
  const suffix = points.slice(segmentIndex + 2);
  return dedupeSequentialPoints([...prefix, ...replacementPoints, ...suffix]);
};

const getExistingSchoolSources = async (pool) => {
  const existing = [];
  for (const source of SCHOOL_SOURCES) {
    if (await tableExists(pool, source.tableName)) {
      existing.push(source);
    }
  }
  return existing;
};

const createObstacleCache = async (db, schoolSources, schoolBuffer) => {
  const schoolParts = schoolSources.map((source) => `
    SELECT
      'school_buffer'::text AS obstacle_type,
      gid AS obstacle_gid,
      ST_Buffer(${quoteIdentifier(source.geomColumn)}, ${schoolBuffer})::geometry(Polygon, 32644) AS obstacle_geom
    FROM ${qualifiedTable(source.tableName)}
    WHERE ${quoteIdentifier(source.geomColumn)} IS NOT NULL
  `);

  const unionSql = [
    ...schoolParts,
    `
      SELECT
        'river'::text AS obstacle_type,
        gid AS obstacle_gid,
        geom AS obstacle_geom
      FROM ${qualifiedTable(TABLES.rivers)}
      WHERE geom IS NOT NULL
    `,
    `
      SELECT
        'mining_site'::text AS obstacle_type,
        gid AS obstacle_gid,
        geom AS obstacle_geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE geom IS NOT NULL
    `,
  ].join(' UNION ALL ');

  await db.query(`DROP TABLE IF EXISTS ${TEMP_OBSTACLE_CACHE}`);
  await db.query(`
    CREATE TEMP TABLE ${TEMP_OBSTACLE_CACHE} AS
    ${unionSql}
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_${TEMP_OBSTACLE_CACHE}_geom
    ON ${TEMP_OBSTACLE_CACHE} USING GIST (obstacle_geom)
  `);
  await db.query(`ANALYZE ${TEMP_OBSTACLE_CACHE}`);
};

const buildObstacleUnionSql = (
  schoolSources,
  miningGid,
  schoolBufferPlaceholder = '$2',
  includeIntersectionsOnly = false,
) => {
  const schoolParts = schoolSources.map((source) => `
    SELECT
      'school_buffer'::text AS obstacle_type,
      gid AS obstacle_gid,
      ST_Buffer(${quoteIdentifier(source.geomColumn)}, ${schoolBufferPlaceholder})::geometry(Polygon, 32644) AS obstacle_geom
    FROM ${qualifiedTable(source.tableName)}
    WHERE ${quoteIdentifier(source.geomColumn)} IS NOT NULL
  `);

  const parts = [
    ...schoolParts,
    `
      SELECT
        'river'::text AS obstacle_type,
        gid AS obstacle_gid,
        geom AS obstacle_geom
      FROM ${qualifiedTable(TABLES.rivers)}
      WHERE geom IS NOT NULL
    `,
    `
      SELECT
        'mining_site'::text AS obstacle_type,
        gid AS obstacle_gid,
        geom AS obstacle_geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid <> ${miningGid}
        AND geom IS NOT NULL
    `,
  ];

  if (!includeIntersectionsOnly) {
    return parts.join(' UNION ALL ');
  }

  return `
    SELECT *
    FROM (${parts.join(' UNION ALL ')}) obstacle_union
    WHERE ST_Intersects(obstacle_geom, route_geom)
  `;
};

const fetchCandidateRoutes = async (pool, miningGid) => {
  const sql = `
    WITH site AS (
      SELECT gid, geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid = $1
    ),
    candidate_roads AS (
      SELECT
        rn.gid AS road_gid,
        rn.road_type,
        ST_ClosestPoint(ST_Boundary(s.geom), rn.geom) AS start_pt,
        ST_ClosestPoint(rn.geom, ST_ClosestPoint(ST_Boundary(s.geom), rn.geom)) AS end_pt
      FROM road_network rn
      CROSS JOIN site s
      WHERE rn.geom IS NOT NULL
      ORDER BY rn.geom <-> s.geom
      LIMIT $2
    )
    SELECT
      road_gid,
      road_type,
      ST_AsText(start_pt) AS start_pt_wkt,
      ST_AsText(end_pt) AS end_pt_wkt,
      ST_Length(ST_MakeLine(start_pt, end_pt)) AS direct_length_m
    FROM candidate_roads
    ORDER BY direct_length_m, road_gid
  `;

  const result = await pool.query(sql, [miningGid, CANDIDATE_LIMIT]);
  return result.rows.map((row) => ({
    ...row,
    startPoint: parsePointWkt(row.start_pt_wkt),
    endPoint: parsePointWkt(row.end_pt_wkt),
  }));
};

const fetchMiningSite = async (pool, miningGid) => {
  const result = await pool.query(
    `SELECT gid, name, district
     FROM ${qualifiedTable(TABLES.miningSites)}
     WHERE gid = $1`,
    [miningGid],
  );
  return result.rows[0] || null;
};

const assessSegmentObstacle = async (db, schoolSources, miningGid, schoolBuffer, segmentPoints, obstacleCacheTable = null) => {
  const segmentWkt = lineWkt(segmentPoints);

  const sql = obstacleCacheTable
    ? `
    WITH route AS (
      SELECT ST_GeomFromText($1, 32644) AS route_geom
    )
    SELECT
      obstacle_type,
      obstacle_gid,
      ST_AsText(obstacle_geom) AS obstacle_wkt,
      ST_Area(ST_Intersection(obstacle_geom, route.route_geom)) AS intersection_area,
      ST_Length(ST_Intersection(obstacle_geom, route.route_geom)) AS intersection_length
    FROM ${obstacleCacheTable}, route
    WHERE ST_Intersects(obstacle_geom, route.route_geom)
      AND NOT (obstacle_type = 'mining_site' AND obstacle_gid = $2)
    ORDER BY intersection_length DESC NULLS LAST, intersection_area DESC NULLS LAST, obstacle_type
    LIMIT 1
  `
    : `
    WITH route AS (
      SELECT ST_GeomFromText($1, 32644) AS route_geom
    ),
    obstacles AS (
      ${buildObstacleUnionSql(schoolSources, miningGid, '$2')}
    )
    SELECT
      obstacle_type,
      obstacle_gid,
      ST_AsText(obstacle_geom) AS obstacle_wkt,
      ST_Area(ST_Intersection(obstacle_geom, route.route_geom)) AS intersection_area,
      ST_Length(ST_Intersection(obstacle_geom, route.route_geom)) AS intersection_length
    FROM obstacles, route
    WHERE ST_Intersects(obstacle_geom, route.route_geom)
    ORDER BY intersection_length DESC NULLS LAST, intersection_area DESC NULLS LAST, obstacle_type
    LIMIT 1
  `;

  const result = await db.query(
    sql,
    obstacleCacheTable ? [segmentWkt, miningGid] : [segmentWkt, schoolBuffer],
  );
  return result.rows[0] || null;
};

const measurePath = async (pool, points) => {
  const sql = `
    SELECT
      ST_Length(ST_GeomFromText($1, 32644)) AS length_m,
      ST_AsGeoJSON(ST_Transform(ST_GeomFromText($1, 32644), 4326))::jsonb AS geometry
  `;
  const result = await pool.query(sql, [lineWkt(points)]);
  return result.rows[0];
};

const createDetourPath = async (
  pool,
  schoolSources,
  miningGid,
  schoolBuffer,
  segmentPoints,
  obstacle,
  obstaclePadding,
  obstacleCacheTable = null,
) => {
  const obstacleUnionSql = obstacleCacheTable
    ? `
      SELECT obstacle_type, obstacle_gid, obstacle_geom
      FROM ${obstacleCacheTable}
      WHERE NOT (obstacle_type = 'mining_site' AND obstacle_gid = ${miningGid})
    `
    : buildObstacleUnionSql(schoolSources, miningGid, '$5');
  const startWkt = pointWkt(segmentPoints[0]);
  const endWkt = pointWkt(segmentPoints[segmentPoints.length - 1]);

  const sql = `
    WITH params AS (
      SELECT
        ST_GeomFromText($1, 32644) AS start_pt,
        ST_GeomFromText($2, 32644) AS end_pt,
        ST_GeomFromText($3, 32644) AS obstacle_geom,
        $4::double precision AS clearance,
        $5::double precision AS school_buffer
    ),
    route_seed AS (
      SELECT ST_MakeLine(start_pt, end_pt) AS route_geom, *
      FROM params
    ),
    obstacle_part AS (
      SELECT
        (ST_Dump(ST_CollectionExtract(ST_Buffer(obstacle_geom, clearance), 3))).geom AS polygon_geom,
        start_pt,
        end_pt,
        route_geom,
        school_buffer
      FROM route_seed
    ),
    chosen_part AS (
      SELECT *
      FROM obstacle_part
      ORDER BY polygon_geom <-> route_geom
      LIMIT 1
    ),
    ring AS (
      SELECT
        ST_ExteriorRing(polygon_geom) AS ring_geom,
        start_pt,
        end_pt,
        school_buffer
      FROM chosen_part
    ),
    snap AS (
      SELECT
        ring_geom,
        start_pt,
        end_pt,
        school_buffer,
        ST_ClosestPoint(ring_geom, start_pt) AS start_snap,
        ST_ClosestPoint(ring_geom, end_pt) AS end_snap
      FROM ring
    ),
    fractions AS (
      SELECT
        ring_geom,
        start_pt,
        end_pt,
        school_buffer,
        start_snap,
        end_snap,
        ST_LineLocatePoint(ring_geom, start_snap) AS f_start,
        ST_LineLocatePoint(ring_geom, end_snap) AS f_end
      FROM snap
    ),
    arcs AS (
      SELECT
        start_pt,
        end_pt,
        school_buffer,
        start_snap,
        end_snap,
        CASE
          WHEN f_start <= f_end THEN ST_LineSubstring(ring_geom, f_start, f_end)
          ELSE ST_MakeLine(ARRAY[
            ST_LineSubstring(ring_geom, f_start, 1),
            ST_LineSubstring(ring_geom, 0, f_end)
          ])
        END AS arc_forward,
        ST_Reverse(
          CASE
            WHEN f_end <= f_start THEN ST_LineSubstring(ring_geom, f_end, f_start)
            ELSE ST_MakeLine(ARRAY[
              ST_LineSubstring(ring_geom, f_end, 1),
              ST_LineSubstring(ring_geom, 0, f_start)
            ])
          END
        ) AS arc_reverse
      FROM fractions
    ),
    candidate_paths AS (
      SELECT
        false AS is_curved,
        ST_RemoveRepeatedPoints(ST_MakeLine(ARRAY[
          ST_MakeLine(start_pt, start_snap),
          arc_forward,
          ST_MakeLine(end_snap, end_pt)
        ]))::geometry(LineString, 32644) AS path_geom,
        school_buffer
      FROM arcs

      UNION ALL

      SELECT
        false AS is_curved,
        ST_RemoveRepeatedPoints(ST_MakeLine(ARRAY[
          ST_MakeLine(start_pt, start_snap),
          arc_reverse,
          ST_MakeLine(end_snap, end_pt)
        ]))::geometry(LineString, 32644) AS path_geom,
        school_buffer
      FROM arcs

      UNION ALL

      SELECT
        true AS is_curved,
        ST_ChaikinSmoothing(
          ST_RemoveRepeatedPoints(ST_MakeLine(ARRAY[
            ST_MakeLine(start_pt, start_snap),
            arc_forward,
            ST_MakeLine(end_snap, end_pt)
          ])),
          2,
          false
        )::geometry(LineString, 32644) AS path_geom,
        school_buffer
      FROM arcs

      UNION ALL

      SELECT
        true AS is_curved,
        ST_ChaikinSmoothing(
          ST_RemoveRepeatedPoints(ST_MakeLine(ARRAY[
            ST_MakeLine(start_pt, start_snap),
            arc_reverse,
            ST_MakeLine(end_snap, end_pt)
          ])),
          2,
          false
        )::geometry(LineString, 32644) AS path_geom,
        school_buffer
      FROM arcs
    ),
    valid_candidates AS (
      SELECT cp.*
      FROM candidate_paths cp
      WHERE NOT EXISTS (
        SELECT 1
        FROM (${obstacleUnionSql}) obstacle_union
        WHERE ST_Intersects(obstacle_union.obstacle_geom, cp.path_geom)
      )
    )
    SELECT
      is_curved,
      ST_AsText(path_geom) AS path_wkt,
      ST_Length(path_geom) AS length_m
    FROM valid_candidates
    ORDER BY length_m
    LIMIT 1
  `;

  const result = await pool.query(sql, [
    startWkt,
    endWkt,
    obstacle.obstacle_wkt,
    obstaclePadding,
    schoolBuffer,
  ]);

  return result.rows[0] || null;
};

const solveCandidatePath = async (pool, schoolSources, miningGid, schoolBuffer, candidate, obstacleCacheTable = null) => {
  let points = [candidate.startPoint, candidate.endPoint];
  let isCurved = false;

  for (let depth = 0; depth <= MAX_DETOUR_DEPTH; depth += 1) {
    let blockingObstacle = null;
    let blockedSegmentIndex = -1;

    for (let segmentIndex = 0; segmentIndex < points.length - 1; segmentIndex += 1) {
      const obstacle = await assessSegmentObstacle(pool, schoolSources, miningGid, schoolBuffer, [
        points[segmentIndex],
        points[segmentIndex + 1],
      ], obstacleCacheTable);

      if (obstacle) {
        blockingObstacle = obstacle;
        blockedSegmentIndex = segmentIndex;
        break;
      }
    }

    if (!blockingObstacle) {
      const measured = await measurePath(pool, points);
      return {
        connected: true,
        roadGid: candidate.road_gid,
        pathLength: Number(measured.length_m),
        pathCost: Number(measured.length_m),
        geometry: measured.geometry,
        geometryWkt: lineWkt(points),
        entryPoint: {
          type: 'Point',
          coordinates: candidate.startPoint,
        },
        connectionPoint: {
          type: 'Point',
          coordinates: candidate.endPoint,
        },
        entryPointWkt: candidate.start_pt_wkt,
        connectionPointWkt: candidate.end_pt_wkt,
        isCurved,
        plannerMode: PLANNER_MODE,
      };
    }

    if (depth === MAX_DETOUR_DEPTH) {
      return {
        connected: false,
        reasonCode:
          blockingObstacle.obstacle_type === 'river'
            ? 'RIVER_BARRIER'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'SCHOOL_BUFFER_BLOCK'
              : 'MINING_SITE_BLOCK',
        reasonDetail:
          blockingObstacle.obstacle_type === 'river'
            ? 'The route still intersects a river polygon after detour attempts.'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'The route still intersects the buffered school exclusion area after detour attempts.'
              : 'The route still intersects other mining polygons after detour attempts.',
      };
    }

    const clearance =
      blockingObstacle.obstacle_type === 'school_buffer'
        ? Math.max(DETOUR_CLEARANCE, schoolBuffer * 0.2)
        : blockingObstacle.obstacle_type === 'river'
          ? Math.max(DETOUR_CLEARANCE, 60)
          : DETOUR_CLEARANCE;

      const detour = await createDetourPath(
        pool,
        schoolSources,
        miningGid,
        schoolBuffer,
        [points[blockedSegmentIndex], points[blockedSegmentIndex + 1]],
        blockingObstacle,
        clearance,
        obstacleCacheTable,
      );

    if (!detour) {
      return {
        connected: false,
        reasonCode:
          blockingObstacle.obstacle_type === 'river'
            ? 'RIVER_BARRIER'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'SCHOOL_BUFFER_BLOCK'
              : 'MINING_SITE_BLOCK',
        reasonDetail:
          blockingObstacle.obstacle_type === 'river'
            ? 'No valid river-end detour candidate was found for the blocked segment.'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'No valid curved bypass was found around the buffered school obstacle.'
              : 'No valid bypass was found around the blocking mining polygon.',
      };
    }

    points = replaceSegmentWithPath(points, blockedSegmentIndex, parseLineStringWkt(detour.path_wkt));
    isCurved = isCurved || detour.is_curved;
  }

  return {
    connected: false,
    reasonCode: 'NO_VALID_PATH',
    reasonDetail: 'No valid connector satisfied the current exclusion rules.',
  };
};

const upsertConnectionStatus = async (pool, payload) => {
  await pool.query(
    `INSERT INTO mining_connection_status (
      mining_gid,
      is_connected,
      connection_road_gid,
      connection_cost,
      path_length,
      path_strategy,
      reason_code,
      reason_detail,
      connected_at,
      entry_point_geom,
      connection_point_geom,
      route_geom,
      metadata
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8,
      CASE WHEN $2 THEN NOW() ELSE NULL END,
      CASE WHEN $9::text IS NULL THEN NULL ELSE ST_GeomFromText($9::text, 32644) END,
      CASE WHEN $10::text IS NULL THEN NULL ELSE ST_GeomFromText($10::text, 32644) END,
      CASE WHEN $11::text IS NULL THEN NULL ELSE ST_GeomFromText($11::text, 32644) END,
      $12::jsonb
    )
    ON CONFLICT (mining_gid)
    DO UPDATE SET
      is_connected = EXCLUDED.is_connected,
      connection_road_gid = EXCLUDED.connection_road_gid,
      connection_cost = EXCLUDED.connection_cost,
      path_length = EXCLUDED.path_length,
      path_strategy = EXCLUDED.path_strategy,
      reason_code = EXCLUDED.reason_code,
      reason_detail = EXCLUDED.reason_detail,
      connected_at = CASE WHEN EXCLUDED.is_connected THEN NOW() ELSE NULL END,
      entry_point_geom = EXCLUDED.entry_point_geom,
      connection_point_geom = EXCLUDED.connection_point_geom,
      route_geom = EXCLUDED.route_geom,
      metadata = EXCLUDED.metadata`,
    [
      payload.miningGid,
      payload.isConnected,
      payload.connectionRoadGid,
      payload.connectionCost,
      payload.pathLength,
      payload.pathStrategy,
      payload.reasonCode,
      payload.reasonDetail,
      payload.entryPointWkt,
      payload.connectionPointWkt,
      payload.geometryWkt,
      JSON.stringify(payload.metadata || {}),
    ],
  );
};

const insertGeneratedRoad = async (pool, miningGid, route) => {
  const result = await pool.query(
    `INSERT INTO road_network (
      road_type,
      source_table,
      source_gid,
      source_mining_site,
      length_km,
      cost,
      reverse_cost,
      geom,
      is_curved,
      is_bypass,
      metadata
    )
    VALUES (
      'mining_access',
      $1,
      $2,
      $2,
      $3 / 1000.0,
      $3,
      $3,
      ST_GeomFromText($4, 32644),
      $5,
      $5,
      $6::jsonb
    )
    RETURNING gid`,
    [
      TABLES.miningSites,
      miningGid,
      route.pathLength,
      route.geometryWkt,
      route.isCurved,
      JSON.stringify({
        connected_to_road_gid: route.roadGid,
        strategy: route.plannerMode,
      }),
    ],
  );

  return result.rows[0].gid;
};

export const calculateRouteForMiningSite = async (
  pool,
  { miningGid, schoolBuffer, persist = false, schoolSources = null, obstacleCacheTable = null },
) => {
  const miningSite = await fetchMiningSite(pool, miningGid);
  if (!miningSite) {
    return null;
  }

  const resolvedSchoolSources = schoolSources || (await getExistingSchoolSources(pool));
  const candidates = await fetchCandidateRoutes(pool, miningGid);
  let bestBlocked = null;

  for (const candidate of candidates) {
    const outcome = await solveCandidatePath(
      pool,
      resolvedSchoolSources,
      miningGid,
      schoolBuffer,
      candidate,
      obstacleCacheTable,
    );
    if (outcome.connected) {
      let generatedRoadGid = null;

      if (persist) {
        generatedRoadGid = await insertGeneratedRoad(pool, miningGid, outcome);
        await upsertConnectionStatus(pool, {
          miningGid,
          isConnected: true,
          connectionRoadGid: generatedRoadGid,
          connectionCost: outcome.pathCost,
          pathLength: outcome.pathLength,
          pathStrategy: outcome.plannerMode,
          reasonCode: null,
          reasonDetail: null,
          entryPointWkt: outcome.entryPointWkt,
          connectionPointWkt: outcome.connectionPointWkt,
          geometryWkt: outcome.geometryWkt,
          metadata: {
            schoolBuffer,
            connectedRoadGid: outcome.roadGid,
            isCurved: outcome.isCurved,
          },
        });
      }

      return {
        miningGid,
        miningName: miningSite.name,
        connected: true,
        schoolBuffer,
        plannerMode: outcome.plannerMode,
        roadGid: outcome.roadGid,
        generatedRoadGid,
        pathLength: outcome.pathLength,
        pathCost: outcome.pathCost,
        geometry: outcome.geometry,
        entryPoint: {
          type: 'Point',
          coordinates: outcome.entryPoint.coordinates,
        },
        connectionPoint: {
          type: 'Point',
          coordinates: outcome.connectionPoint.coordinates,
        },
        isCurved: outcome.isCurved,
      };
    }

    if (!bestBlocked) {
      bestBlocked = outcome;
    }
  }

  const blockedResponse = {
    miningGid,
    miningName: miningSite.name,
    connected: false,
    schoolBuffer,
    reasonCode: bestBlocked?.reasonCode || 'NO_VALID_PATH',
    reasonDetail: bestBlocked?.reasonDetail || 'No valid connector satisfied the current exclusion rules.',
    plannerMode: PLANNER_MODE,
    attemptedCandidates: candidates.length,
  };

  if (persist) {
    await upsertConnectionStatus(pool, {
      miningGid,
      isConnected: false,
      connectionRoadGid: null,
      connectionCost: null,
      pathLength: null,
      pathStrategy: PLANNER_MODE,
      reasonCode: blockedResponse.reasonCode,
      reasonDetail: blockedResponse.reasonDetail,
      entryPointWkt: null,
      connectionPointWkt: null,
      geometryWkt: null,
      metadata: {
        schoolBuffer,
        attemptedCandidates: candidates.length,
      },
    });
  }

  return blockedResponse;
};

export const generateRoutesForMiningSites = async (pool, { batchSize, schoolBuffer }) => {
  const client = await pool.connect();

  try {
    await reseedBaseRoadNetwork(client);
    const schoolSources = await getExistingSchoolSources(client);
    await createObstacleCache(client, schoolSources, schoolBuffer);

    const totalResult = await client.query(`SELECT COUNT(*)::int AS count FROM ${qualifiedTable(TABLES.miningSites)}`);
    const totalSites = totalResult.rows[0].count;
    const limit = batchSize && batchSize > 0 ? Math.min(batchSize, totalSites) : totalSites;

    const miningResult = await client.query(
      `SELECT gid
       FROM ${qualifiedTable(TABLES.miningSites)}
       WHERE geom IS NOT NULL
       ORDER BY gid
       LIMIT $1`,
      [limit],
    );

    let processedCount = 0;
    let totalRoadLength = 0;
    const failedDetails = [];

    for (const row of miningResult.rows) {
      const outcome = await calculateRouteForMiningSite(client, {
        miningGid: row.gid,
        schoolBuffer,
        persist: true,
        schoolSources,
        obstacleCacheTable: TEMP_OBSTACLE_CACHE,
      });

      if (!outcome) {
        failedDetails.push({
          gid: row.gid,
          reason: 'Mining site not found during processing.',
        });
        continue;
      }

      if (outcome.connected) {
        processedCount += 1;
        totalRoadLength += outcome.pathLength;
      } else {
        failedDetails.push({
          gid: row.gid,
          reason: outcome.reasonDetail,
          code: outcome.reasonCode,
        });
      }
    }

    return {
      success: failedDetails.length === 0,
      message:
        failedDetails.length === 0
          ? 'All selected mining sites received an obstacle-aware connector.'
          : 'Some sites are still blocked after curved detour attempts.',
      plannerMode: PLANNER_MODE,
      processedCount,
      totalRoadLength,
      failedCount: failedDetails.length,
      failedDetails,
      totalSites,
      selectedSites: limit,
      schoolBuffer,
    };
  } finally {
    client.release();
  }
};

export const resetRoadNetwork = async (pool) => {
  await reseedBaseRoadNetwork(pool);
};
