import { SCHOOL_SOURCES, TABLES } from '../config/datasets.js';
import { qualifiedTable, quoteIdentifier, tableExists } from '../utils/sql.js';
import { reseedBaseRoadNetwork } from './bootstrap.js';

const CANDIDATE_LIMIT = 14;
const CANDIDATE_LIMIT_STEPS = [14, 40, 80, 140];
const MAX_DETOUR_DEPTH = 6;
const DETOUR_CLEARANCE = 30;
const PLANNER_MODE = 'obstacle_aware_polyline';
const TEMP_OBSTACLE_CACHE = 'temp_route_obstacle_cache';
const BASE_ROAD_SHARE = 0.65;

const pointWkt = ([x, y]) => `POINT(${x} ${y})`;
const lineWkt = (points) => `LINESTRING(${points.map(([x, y]) => `${x} ${y}`).join(', ')})`;
const GRID_CELL_SIZE = 120;
const GRID_MARGIN_MIN = 1500;
const GRID_MARGIN_MAX = 10000;
const GRID_MAX_DIMENSION = 120;

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

const getEnvelope = (points, margin = 0) => {
  const xs = points.map(([x]) => x);
  const ys = points.map(([, y]) => y);
  return {
    minX: Math.min(...xs) - margin,
    minY: Math.min(...ys) - margin,
    maxX: Math.max(...xs) + margin,
    maxY: Math.max(...ys) + margin,
  };
};

const distance = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1]);

const pointInRing = (point, ring) => {
  const [px, py] = point;
  let inside = false;

  for (let i = 0, j = ring.length - 1; i < ring.length; j = i, i += 1) {
    const [xi, yi] = ring[i];
    const [xj, yj] = ring[j];
    const intersects = ((yi > py) !== (yj > py))
      && (px < ((xj - xi) * (py - yi)) / ((yj - yi) || Number.EPSILON) + xi);
    if (intersects) inside = !inside;
  }

  return inside;
};

const pointInPolygonCoordinates = (point, polygonCoords) => {
  if (!pointInRing(point, polygonCoords[0])) {
    return false;
  }

  for (let i = 1; i < polygonCoords.length; i += 1) {
    if (pointInRing(point, polygonCoords[i])) {
      return false;
    }
  }

  return true;
};

const orientation = (a, b, c) => {
  const value = (b[1] - a[1]) * (c[0] - b[0]) - (b[0] - a[0]) * (c[1] - b[1]);
  if (Math.abs(value) < 1e-9) return 0;
  return value > 0 ? 1 : 2;
};

const onSegment = (a, b, c) =>
  Math.min(a[0], c[0]) <= b[0] && b[0] <= Math.max(a[0], c[0])
  && Math.min(a[1], c[1]) <= b[1] && b[1] <= Math.max(a[1], c[1]);

const segmentsIntersect = (p1, q1, p2, q2) => {
  const o1 = orientation(p1, q1, p2);
  const o2 = orientation(p1, q1, q2);
  const o3 = orientation(p2, q2, p1);
  const o4 = orientation(p2, q2, q1);

  if (o1 !== o2 && o3 !== o4) return true;
  if (o1 === 0 && onSegment(p1, p2, q1)) return true;
  if (o2 === 0 && onSegment(p1, q2, q1)) return true;
  if (o3 === 0 && onSegment(p2, p1, q2)) return true;
  if (o4 === 0 && onSegment(p2, q1, q2)) return true;
  return false;
};

const segmentIntersectsRing = (a, b, ring) => {
  for (let i = 0; i < ring.length - 1; i += 1) {
    if (segmentsIntersect(a, b, ring[i], ring[i + 1])) {
      return true;
    }
  }
  return false;
};

const geometryContainsPoint = (geometry, point) => {
  if (!geometry) return false;
  if (geometry.type === 'Polygon') {
    return pointInPolygonCoordinates(point, geometry.coordinates);
  }
  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.some((polygon) => pointInPolygonCoordinates(point, polygon));
  }
  return false;
};

const geometryIntersectsSegment = (geometry, a, b) => {
  if (!geometry) return false;

  const polygons = geometry.type === 'Polygon'
    ? [geometry.coordinates]
    : geometry.type === 'MultiPolygon'
      ? geometry.coordinates
      : [];

  return polygons.some((polygon) => {
    if (pointInPolygonCoordinates(a, polygon) || pointInPolygonCoordinates(b, polygon)) {
      return true;
    }

    return polygon.some((ring) => segmentIntersectsRing(a, b, ring));
  });
};

const parseObstacleGeometries = (rows) => rows.map((row) => ({
  obstacleType: row.obstacle_type,
  obstacleGid: row.obstacle_gid,
  geometry: row.geometry,
}));

const buildPathFromParents = (cameFrom, currentKey, nodesByKey) => {
  const path = [nodesByKey.get(currentKey)];
  let cursor = currentKey;
  while (cameFrom.has(cursor)) {
    cursor = cameFrom.get(cursor);
    path.push(nodesByKey.get(cursor));
  }
  return path.reverse();
};

const smoothPath = (path, obstacles) => {
  if (path.length <= 2) return path;

  const smoothed = [path[0]];
  let anchorIndex = 0;

  while (anchorIndex < path.length - 1) {
    let bestIndex = anchorIndex + 1;

    for (let i = path.length - 1; i > anchorIndex + 1; i -= 1) {
      const clear = obstacles.every(
        (obstacle) => !geometryIntersectsSegment(obstacle.geometry, path[anchorIndex], path[i]),
      );
      if (clear) {
        bestIndex = i;
        break;
      }
    }

    smoothed.push(path[bestIndex]);
    anchorIndex = bestIndex;
  }

  return dedupeSequentialPoints(smoothed);
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

const fetchCandidateRoutes = async (pool, miningGid, limit = CANDIDATE_LIMIT) => {
  const baseLimit = Math.max(6, Math.ceil(limit * BASE_ROAD_SHARE));
  const generatedLimit = Math.max(4, limit - baseLimit);
  const sql = `
    WITH site AS (
      SELECT gid, geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid = $1
    ),
    source_candidates AS (
      SELECT
        rn.gid AS road_gid,
        rn.road_type,
        ST_ClosestPoint(ST_Boundary(s.geom), rn.geom) AS start_pt,
        ST_ClosestPoint(rn.geom, ST_ClosestPoint(ST_Boundary(s.geom), rn.geom)) AS end_pt
      FROM road_network rn
      CROSS JOIN site s
      WHERE rn.geom IS NOT NULL
        AND rn.source_mining_site IS NULL
      ORDER BY rn.geom <-> s.geom
      LIMIT $2
    ),
    generated_candidates AS (
      SELECT
        rn.gid AS road_gid,
        rn.road_type,
        ST_ClosestPoint(ST_Boundary(s.geom), rn.geom) AS start_pt,
        ST_ClosestPoint(rn.geom, ST_ClosestPoint(ST_Boundary(s.geom), rn.geom)) AS end_pt
      FROM road_network rn
      CROSS JOIN site s
      WHERE rn.geom IS NOT NULL
        AND rn.source_mining_site IS NOT NULL
      ORDER BY rn.geom <-> s.geom
      LIMIT $3
    ),
    candidate_roads AS (
      SELECT * FROM source_candidates
      UNION ALL
      SELECT * FROM generated_candidates
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

  const result = await pool.query(sql, [miningGid, baseLimit, generatedLimit]);
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

const fetchLocalObstacles = async (db, miningGid, envelope, obstacleCacheTable = null, schoolSources = null, schoolBuffer = null) => {
  const sql = obstacleCacheTable
    ? `
      SELECT
        obstacle_type,
        obstacle_gid,
        ST_AsGeoJSON(ST_SimplifyPreserveTopology(obstacle_geom, 15))::jsonb AS geometry
      FROM ${obstacleCacheTable}
      WHERE ST_Intersects(
        obstacle_geom,
        ST_MakeEnvelope($1, $2, $3, $4, 32644)
      )
      AND NOT (obstacle_type = 'mining_site' AND obstacle_gid = $5)
    `
    : `
      SELECT
        obstacle_type,
        obstacle_gid,
        ST_AsGeoJSON(ST_SimplifyPreserveTopology(obstacle_geom, 15))::jsonb AS geometry
      FROM (${buildObstacleUnionSql(schoolSources, miningGid, '$5')}) obstacle_union
      WHERE ST_Intersects(
        obstacle_geom,
        ST_MakeEnvelope($1, $2, $3, $4, 32644)
      )
    `;

  const params = obstacleCacheTable
    ? [envelope.minX, envelope.minY, envelope.maxX, envelope.maxY, miningGid]
    : [envelope.minX, envelope.minY, envelope.maxX, envelope.maxY, schoolBuffer];

  const result = await db.query(sql, params);
  return parseObstacleGeometries(result.rows);
};

const findClosestFreeGridPoint = (point, blockedSet, nodesByKey, cols, rows) => {
  let bestNode = null;
  let bestDistance = Number.POSITIVE_INFINITY;

  for (let row = 0; row < rows; row += 1) {
    for (let col = 0; col < cols; col += 1) {
      const key = `${col}:${row}`;
      if (blockedSet.has(key)) continue;
      const node = nodesByKey.get(key);
      const d = distance(point, node);
      if (d < bestDistance) {
        bestDistance = d;
        bestNode = { key, point: node };
      }
    }
  }

  return bestNode;
};

const solveWithGridFallback = async (
  db,
  miningGid,
  schoolBuffer,
  candidate,
  obstacleCacheTable,
  schoolSources,
) => {
  const directDistance = distance(candidate.startPoint, candidate.endPoint);
  const margin = Math.max(GRID_MARGIN_MIN, Math.min(GRID_MARGIN_MAX, directDistance * 0.8));
  const envelope = getEnvelope([candidate.startPoint, candidate.endPoint], margin);
  const width = envelope.maxX - envelope.minX;
  const height = envelope.maxY - envelope.minY;

  let cellSize = GRID_CELL_SIZE;
  while ((width / cellSize > GRID_MAX_DIMENSION || height / cellSize > GRID_MAX_DIMENSION) && cellSize < 300) {
    cellSize += 20;
  }

  const cols = Math.max(3, Math.ceil(width / cellSize) + 1);
  const rows = Math.max(3, Math.ceil(height / cellSize) + 1);
  const obstacles = await fetchLocalObstacles(
    db,
    miningGid,
    envelope,
    obstacleCacheTable,
    schoolSources,
    schoolBuffer,
  );

  const nodesByKey = new Map();
  const blockedSet = new Set();

  for (let row = 0; row < rows; row += 1) {
    for (let col = 0; col < cols; col += 1) {
      const point = [
        envelope.minX + col * cellSize,
        envelope.minY + row * cellSize,
      ];
      const key = `${col}:${row}`;
      nodesByKey.set(key, point);

      if (obstacles.some((obstacle) => geometryContainsPoint(obstacle.geometry, point))) {
        blockedSet.add(key);
      }
    }
  }

  const startNode = findClosestFreeGridPoint(candidate.startPoint, blockedSet, nodesByKey, cols, rows);
  const endNode = findClosestFreeGridPoint(candidate.endPoint, blockedSet, nodesByKey, cols, rows);

  if (!startNode || !endNode) {
    return null;
  }

  const open = new Set([startNode.key]);
  const cameFrom = new Map();
  const gScore = new Map([[startNode.key, 0]]);
  const fScore = new Map([[startNode.key, distance(startNode.point, endNode.point)]]);

  while (open.size > 0) {
    let currentKey = null;
    let currentScore = Number.POSITIVE_INFINITY;
    for (const key of open) {
      const score = fScore.get(key) ?? Number.POSITIVE_INFINITY;
      if (score < currentScore) {
        currentScore = score;
        currentKey = key;
      }
    }

    if (!currentKey) break;
    if (currentKey === endNode.key) {
      const gridPath = buildPathFromParents(cameFrom, currentKey, nodesByKey);
      const fullPath = dedupeSequentialPoints([
        candidate.startPoint,
        ...gridPath,
        candidate.endPoint,
      ]);
      const smoothed = smoothPath(fullPath, obstacles);
      if (smoothed.length < 2) {
        return null;
      }
      const measured = await measurePath(db, smoothed);
      return {
        connected: true,
        roadGid: candidate.road_gid,
        pathLength: Number(measured.length_m),
        pathCost: Number(measured.length_m),
        geometry: measured.geometry,
        geometryWkt: lineWkt(smoothed),
        entryPoint: { type: 'Point', coordinates: candidate.startPoint },
        connectionPoint: { type: 'Point', coordinates: candidate.endPoint },
        entryPointWkt: candidate.start_pt_wkt,
        connectionPointWkt: candidate.end_pt_wkt,
        isCurved: smoothed.length > 2,
        plannerMode: 'grid_fallback_shortest_route',
      };
    }

    open.delete(currentKey);
    const [col, row] = currentKey.split(':').map(Number);
    const currentPoint = nodesByKey.get(currentKey);

    for (let dCol = -1; dCol <= 1; dCol += 1) {
      for (let dRow = -1; dRow <= 1; dRow += 1) {
        if (dCol === 0 && dRow === 0) continue;
        const nextCol = col + dCol;
        const nextRow = row + dRow;
        if (nextCol < 0 || nextCol >= cols || nextRow < 0 || nextRow >= rows) continue;

        const neighborKey = `${nextCol}:${nextRow}`;
        if (blockedSet.has(neighborKey)) continue;

        const neighborPoint = nodesByKey.get(neighborKey);
        if (obstacles.some((obstacle) => geometryIntersectsSegment(obstacle.geometry, currentPoint, neighborPoint))) {
          continue;
        }

        const tentativeG = (gScore.get(currentKey) ?? Number.POSITIVE_INFINITY) + distance(currentPoint, neighborPoint);
        if (tentativeG < (gScore.get(neighborKey) ?? Number.POSITIVE_INFINITY)) {
          cameFrom.set(neighborKey, currentKey);
          gScore.set(neighborKey, tentativeG);
          fScore.set(neighborKey, tentativeG + distance(neighborPoint, endNode.point));
          open.add(neighborKey);
        }
      }
    }
  }

  return null;
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
  if (!obstacleCacheTable && typeof pool.connect === 'function' && typeof pool.release !== 'function') {
    const client = await pool.connect();
    try {
      const cachedSchoolSources = schoolSources || (await getExistingSchoolSources(client));
      await createObstacleCache(client, cachedSchoolSources, schoolBuffer);
      return await calculateRouteForMiningSite(client, {
        miningGid,
        schoolBuffer,
        persist,
        schoolSources: cachedSchoolSources,
        obstacleCacheTable: TEMP_OBSTACLE_CACHE,
      });
    } finally {
      client.release();
    }
  }

  const miningSite = await fetchMiningSite(pool, miningGid);
  if (!miningSite) {
    return null;
  }

  const resolvedSchoolSources = schoolSources || (await getExistingSchoolSources(pool));
  let bestConnected = null;
  let bestBlocked = null;
  const seenRoads = new Set();
  const candidatePool = [];

  for (const limit of CANDIDATE_LIMIT_STEPS) {
    const candidates = await fetchCandidateRoutes(pool, miningGid, limit);

    for (const candidate of candidates) {
      if (seenRoads.has(candidate.road_gid)) {
        continue;
      }
      seenRoads.add(candidate.road_gid);
      candidatePool.push(candidate);

      if (bestConnected && Number(candidate.direct_length_m) >= bestConnected.pathLength) {
        continue;
      }

      const outcome = await solveCandidatePath(
        pool,
        resolvedSchoolSources,
        miningGid,
        schoolBuffer,
        candidate,
        obstacleCacheTable,
      );

      if (outcome.connected && (!bestConnected || outcome.pathLength < bestConnected.pathLength)) {
        bestConnected = outcome;
      }

      if (!outcome.connected && !bestBlocked) {
        bestBlocked = outcome;
      }
    }

    if (bestConnected) {
      break;
    }
  }

  if (!bestConnected) {
    for (const candidate of candidatePool.slice(0, 6)) {
      const gridOutcome = await solveWithGridFallback(
        pool,
        miningGid,
        schoolBuffer,
        candidate,
        obstacleCacheTable,
        resolvedSchoolSources,
      );

      if (gridOutcome && (!bestConnected || gridOutcome.pathLength < bestConnected.pathLength)) {
        bestConnected = gridOutcome;
      }
    }
  }

  if (bestConnected) {
    let generatedRoadGid = null;

    if (persist) {
      generatedRoadGid = await insertGeneratedRoad(pool, miningGid, bestConnected);
      await upsertConnectionStatus(pool, {
        miningGid,
        isConnected: true,
        connectionRoadGid: generatedRoadGid,
        connectionCost: bestConnected.pathCost,
        pathLength: bestConnected.pathLength,
        pathStrategy: bestConnected.plannerMode,
        reasonCode: null,
        reasonDetail: null,
        entryPointWkt: bestConnected.entryPointWkt,
        connectionPointWkt: bestConnected.connectionPointWkt,
        geometryWkt: bestConnected.geometryWkt,
        metadata: {
          schoolBuffer,
          connectedRoadGid: bestConnected.roadGid,
          isCurved: bestConnected.isCurved,
        },
      });
    }

    return {
      miningGid,
      miningName: miningSite.name,
      connected: true,
      schoolBuffer,
      plannerMode: bestConnected.plannerMode,
      roadGid: bestConnected.roadGid,
      generatedRoadGid,
      pathLength: bestConnected.pathLength,
      pathCost: bestConnected.pathCost,
      geometry: bestConnected.geometry,
      entryPoint: {
        type: 'Point',
        coordinates: bestConnected.entryPoint.coordinates,
      },
      connectionPoint: {
        type: 'Point',
        coordinates: bestConnected.connectionPoint.coordinates,
      },
      isCurved: bestConnected.isCurved,
    };
  }

  const blockedResponse = {
    miningGid,
    miningName: miningSite.name,
    connected: false,
    schoolBuffer,
    reasonCode: bestBlocked?.reasonCode || 'NO_VALID_PATH',
    reasonDetail: bestBlocked?.reasonDetail || 'No valid connector satisfied the current exclusion rules.',
    plannerMode: PLANNER_MODE,
    attemptedCandidates: seenRoads.size,
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
        attemptedCandidates: seenRoads.size,
      },
    });
  }

  return blockedResponse;
};

export const generateRoutesForMiningSites = async (pool, { batchSize, schoolBuffer, appendMode = true }) => {
  const client = await pool.connect();

  try {
    if (!appendMode) {
      await reseedBaseRoadNetwork(client);
    }
    const schoolSources = await getExistingSchoolSources(client);
    await createObstacleCache(client, schoolSources, schoolBuffer);

    const totalResult = await client.query(`SELECT COUNT(*)::int AS count FROM ${qualifiedTable(TABLES.miningSites)}`);
    const totalSites = totalResult.rows[0].count;
    const limit = batchSize && batchSize > 0 ? Math.min(batchSize, totalSites) : totalSites;
    const blockedResult = await client.query(`
      SELECT COUNT(*)::int AS count
      FROM mining_connection_status
      WHERE NOT is_connected
    `);
    const blockedCount = blockedResult.rows[0].count;
    const blockedQuota = limit > 1 && blockedCount > 0 ? Math.max(1, Math.floor(limit * 0.25)) : 0;
    const pendingQuota = Math.max(1, limit - blockedQuota);

    const miningResult = await client.query(
      `WITH unprocessed AS (
         SELECT gb.gid, 0 AS priority
         FROM ${qualifiedTable(TABLES.miningSites)} gb
         LEFT JOIN mining_connection_status mcs
           ON mcs.mining_gid = gb.gid
         WHERE gb.geom IS NOT NULL
           AND mcs.mining_gid IS NULL
         ORDER BY gb.gid
         LIMIT $1
       ),
       blocked_retry AS (
         SELECT gb.gid, 1 AS priority
         FROM ${qualifiedTable(TABLES.miningSites)} gb
         JOIN mining_connection_status mcs
           ON mcs.mining_gid = gb.gid
         WHERE gb.geom IS NOT NULL
           AND NOT mcs.is_connected
         ORDER BY gb.gid
         LIMIT $2
       )
       SELECT gid
       FROM (
         SELECT * FROM unprocessed
         UNION ALL
         SELECT * FROM blocked_retry
       ) selected
       ORDER BY priority, gid
       LIMIT $3`,
      [pendingQuota, blockedQuota || limit, limit],
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
      appendMode,
    };
  } finally {
    client.release();
  }
};

export const resetRoadNetwork = async (pool) => {
  await reseedBaseRoadNetwork(pool);
};
