import { MINING_SITE_FILTER, SCHOOL_SOURCES, TABLES } from '../config/datasets.js';
import { qualifiedTable, quoteIdentifier, quoteLiteral, tableExists } from '../utils/sql.js';
import { reseedBaseRoadNetwork } from './bootstrap.js';

const CANDIDATE_LIMIT = 12;
const QUICK_CANDIDATE_LIMIT_STEPS = [12, 24];
const EXTENDED_CANDIDATE_LIMIT_STEPS = [48, 96];
const MAX_DETOUR_DEPTH = 6;
const DETOUR_CLEARANCE = 30;
const PLANNER_MODE = 'obstacle_aware_polyline';
const TEMP_OBSTACLE_CACHE = 'temp_route_obstacle_cache';
const GRID_FALLBACK_CANDIDATES = 4;
const MAX_CANDIDATE_PATH_EVALUATIONS = 36;
export const MAX_BATCH_SELECTION = 60;
export const DEFAULT_BATCH_SELECTION = 4;
const MAX_GRID_OBSTACLES = 400;

const pointWkt = ([x, y]) => `POINT(${x} ${y})`;
const lineWkt = (points) => `LINESTRING(${points.map(([x, y]) => `${x} ${y}`).join(', ')})`;
const GRID_CELL_SIZE = 120;
const GRID_MARGIN_MIN = 1500;
const GRID_MARGIN_MAX = 10000;
const GRID_MAX_DIMENSION = 120;
const LONG_ROUTE_CELL_SIZE = 220;
const LONG_ROUTE_DISTANCE_THRESHOLD = 2500;
const LONG_ROUTE_GRID_MAX_DIMENSION = 80;
const RIVER_GRID_MARGIN_MIN = 4000;
const RIVER_GRID_MARGIN_MAX = 18000;
const ROAD_BARRIER_BUFFER = 12;
const SHORT_CONNECTOR_DIRECT_DISTANCE_MAX = 100;
const SHORT_CONNECTOR_PATH_LENGTH_MAX = 250;
const MAX_SHORT_CONNECTOR_DETOUR_RATIO = 6;
const SHORT_CANDIDATE_SKIP_DISTANCE_MAX = 150;
const FULL_INTERSECTION_RATIO = 0.85;
const TURN_PENALTY_METERS = 120;
const ACUTE_TURN_PENALTY_METERS = 120;

const buildMiningSiteWhereClause = (tableAlias = null) => {
  const columnPrefix = tableAlias ? `${tableAlias}.` : '';
  const values = MINING_SITE_FILTER.includedValues.map((value) => quoteLiteral(value)).join(', ');
  return `${columnPrefix}${quoteIdentifier(MINING_SITE_FILTER.nameColumn)} IN (${values})`;
};

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

const dotProduct = (a, b) => a[0] * b[0] + a[1] * b[1];

const vectorBetween = (from, to) => [to[0] - from[0], to[1] - from[1]];

const vectorMagnitude = (vector) => Math.hypot(vector[0], vector[1]);

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

const getTurnMetrics = (points) => {
  let turnCount = 0;
  let acuteTurnCount = 0;

  for (let index = 1; index < points.length - 1; index += 1) {
    const incoming = vectorBetween(points[index], points[index - 1]);
    const outgoing = vectorBetween(points[index], points[index + 1]);
    const incomingMagnitude = vectorMagnitude(incoming);
    const outgoingMagnitude = vectorMagnitude(outgoing);

    if (incomingMagnitude < 1e-6 || outgoingMagnitude < 1e-6) {
      continue;
    }

    const cosine = dotProduct(incoming, outgoing) / (incomingMagnitude * outgoingMagnitude);
    const clampedCosine = Math.max(-1, Math.min(1, cosine));
    const turnAngle = 180 - ((Math.acos(clampedCosine) * 180) / Math.PI);

    if (turnAngle >= 20) {
      turnCount += 1;
    }

    if (turnAngle >= 60) {
      acuteTurnCount += 1;
    }
  }

  return { turnCount, acuteTurnCount };
};

const calculatePathCost = (points, pathLength) => {
  const { turnCount, acuteTurnCount } = getTurnMetrics(points);
  return {
    turnCount,
    acuteTurnCount,
    pathCost:
      pathLength
      + (turnCount * TURN_PENALTY_METERS)
      + (acuteTurnCount * ACUTE_TURN_PENALTY_METERS),
  };
};

const isRouteQualityAcceptable = (route) => {
  if (!route?.connected || !route?.entryPoint?.coordinates || !route?.connectionPoint?.coordinates) {
    return false;
  }

  const directDistance = distance(route.entryPoint.coordinates, route.connectionPoint.coordinates);
  if (directDistance <= 0) {
    return true;
  }

  const detourRatio = Number(route.pathLength) / directDistance;
  return !(
    directDistance <= SHORT_CONNECTOR_DIRECT_DISTANCE_MAX
    && Number(route.pathLength) >= SHORT_CONNECTOR_PATH_LENGTH_MAX
    && detourRatio > MAX_SHORT_CONNECTOR_DETOUR_RATIO
  );
};

const shouldSkipBlockedShortCandidate = (candidate, obstacle) => {
  if (!obstacle) {
    return false;
  }

  const directLength = Number(candidate.direct_length_m || 0);
  const intersectionLength = Number(obstacle.intersection_length || 0);

  return (
    directLength > 0
    && directLength <= SHORT_CANDIDATE_SKIP_DISTANCE_MAX
    && obstacle.obstacle_type === 'school_buffer'
    && intersectionLength >= directLength * FULL_INTERSECTION_RATIO
  );
};

const optimizePathShape = async (
  db,
  miningGid,
  schoolBuffer,
  points,
  obstacleCacheTable = null,
  schoolSources = null,
  margin = GRID_MARGIN_MIN,
  excludeRoadGid = null,
) => {
  if (points.length <= 2) {
    return points;
  }

  const envelope = getEnvelope(points, margin);
  const obstacles = await fetchLocalObstacles(
    db,
    miningGid,
    envelope,
    obstacleCacheTable,
    schoolSources,
    schoolBuffer,
    excludeRoadGid,
  );

  if (obstacles.length === 0) {
    return [points[0], points[points.length - 1]];
  }

  return smoothPath(points, obstacles);
};

const replaceSegmentWithPath = (points, segmentIndex, replacementPoints) => {
  const prefix = points.slice(0, segmentIndex);
  const suffix = points.slice(segmentIndex + 2);
  return dedupeSequentialPoints([...prefix, ...replacementPoints, ...suffix]);
};

const getFallbackCandidates = (candidatePool, limit = GRID_FALLBACK_CANDIDATES) =>
  candidatePool
    .slice()
    .sort((a, b) => {
      const directGap = Number(a.direct_length_m) - Number(b.direct_length_m);
      if (directGap !== 0) return directGap;
      return Number(a.boundary_distance || 0) - Number(b.boundary_distance || 0);
    })
    .slice(0, limit);

const getExistingSchoolSources = async (pool) => {
  const existing = [];
  for (const source of SCHOOL_SOURCES) {
    if (await tableExists(pool, source.tableName)) {
      existing.push(source);
    }
  }
  return existing;
};

const getObstacleSchoolSources = (sources) => {
  const filtered = sources.filter((source) => source.useForObstacles !== false);
  const bufferedSources = filtered.filter((source) => source.isBuffered);

  if (bufferedSources.length > 0) {
    return [
      ...bufferedSources,
      ...filtered.filter((source) => !source.isBuffered && source.tableName === 'gorakhpur_ps'),
    ];
  }

  return filtered;
};

const createObstacleCache = async (db, schoolSources, schoolBuffer) => {
  const schoolParts = schoolSources.map((source) => `
    SELECT
      'school_buffer'::text AS obstacle_type,
      ${quoteIdentifier(source.idColumn || 'gid')} AS obstacle_gid,
      ${
        source.isBuffered
          ? `${quoteIdentifier(source.geomColumn)}::geometry`
          : `ST_Buffer(${quoteIdentifier(source.geomColumn)}, ${schoolBuffer})::geometry(Polygon, 32644)`
      } AS obstacle_geom
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
        AND ${buildMiningSiteWhereClause()}
    `,
    `
      SELECT
        'forbidden_mining'::text AS obstacle_type,
        ROW_NUMBER() OVER ()::integer AS obstacle_gid,
        geom AS obstacle_geom
      FROM ${qualifiedTable(TABLES.forbiddenMining)}
      WHERE geom IS NOT NULL
    `,
    `
      SELECT
        'road_barrier'::text AS obstacle_type,
        gid AS obstacle_gid,
        ST_Buffer(geom, ${ROAD_BARRIER_BUFFER})::geometry(Polygon, 32644) AS obstacle_geom
      FROM road_network
      WHERE geom IS NOT NULL
        AND source_mining_site IS NULL
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
  excludeRoadGidLiteral = 'NULL',
  includeIntersectionsOnly = false,
) => {
  const schoolParts = schoolSources.map((source) => `
    SELECT
      'school_buffer'::text AS obstacle_type,
      ${quoteIdentifier(source.idColumn || 'gid')} AS obstacle_gid,
      ${
        source.isBuffered
          ? `${quoteIdentifier(source.geomColumn)}::geometry`
          : `ST_Buffer(${quoteIdentifier(source.geomColumn)}, ${schoolBufferPlaceholder})::geometry(Polygon, 32644)`
      } AS obstacle_geom
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
        AND ${buildMiningSiteWhereClause()}
    `,
    `
      SELECT
        'forbidden_mining'::text AS obstacle_type,
        ROW_NUMBER() OVER ()::integer AS obstacle_gid,
        geom AS obstacle_geom
      FROM ${qualifiedTable(TABLES.forbiddenMining)}
      WHERE geom IS NOT NULL
    `,
    `
      SELECT
        'road_barrier'::text AS obstacle_type,
        gid AS obstacle_gid,
        ST_Buffer(geom, ${ROAD_BARRIER_BUFFER})::geometry(Polygon, 32644) AS obstacle_geom
      FROM road_network
      WHERE geom IS NOT NULL
        AND source_mining_site IS NULL
        AND gid <> COALESCE(${excludeRoadGidLiteral}, -1)
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
  const sql = `
    WITH site AS (
      SELECT
        gid,
        geom,
        ST_Boundary(geom) AS boundary_geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid = $1
        AND ${buildMiningSiteWhereClause()}
    ),
    ranked_roads AS (
      SELECT
        rn.gid AS road_gid,
        rn.road_type,
        ST_ClosestPoint(s.boundary_geom, rn.geom) AS start_pt,
        ST_ClosestPoint(rn.geom, ST_ClosestPoint(s.boundary_geom, rn.geom)) AS end_pt,
        ST_Distance(rn.geom, s.boundary_geom) AS boundary_distance,
        ROW_NUMBER() OVER (
          ORDER BY
            ST_Distance(rn.geom, s.boundary_geom),
            rn.geom <-> s.boundary_geom,
            rn.gid
        ) AS road_rank
      FROM road_network rn
      CROSS JOIN site s
      WHERE rn.geom IS NOT NULL
        AND (rn.source_mining_site IS NULL OR rn.source_mining_site <> $1)
    ),
    candidate_roads AS (
      SELECT *
      FROM ranked_roads
      WHERE road_rank <= $2
    )
    SELECT
      road_gid,
      road_type,
      ST_AsText(start_pt) AS start_pt_wkt,
      ST_AsText(end_pt) AS end_pt_wkt,
      boundary_distance,
      ST_Length(ST_MakeLine(start_pt, end_pt)) AS direct_length_m
    FROM candidate_roads
    ORDER BY direct_length_m, boundary_distance, road_gid
  `;

  const result = await pool.query(sql, [miningGid, limit]);
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
     WHERE gid = $1
       AND ${buildMiningSiteWhereClause()}`,
    [miningGid],
  );
  return result.rows[0] || null;
};

const fetchLocalObstacles = async (
  db,
  miningGid,
  envelope,
  obstacleCacheTable = null,
  schoolSources = null,
  schoolBuffer = null,
  excludeRoadGid = null,
) => {
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
      AND NOT (obstacle_type = 'road_barrier' AND obstacle_gid = COALESCE($6, -1))
    `
    : `
      SELECT
        obstacle_type,
        obstacle_gid,
        ST_AsGeoJSON(ST_SimplifyPreserveTopology(obstacle_geom, 15))::jsonb AS geometry
      FROM (${buildObstacleUnionSql(schoolSources, miningGid, '$5', '$6')}) obstacle_union
      WHERE ST_Intersects(
        obstacle_geom,
        ST_MakeEnvelope($1, $2, $3, $4, 32644)
      )
    `;

  const params = obstacleCacheTable
    ? [envelope.minX, envelope.minY, envelope.maxX, envelope.maxY, miningGid, excludeRoadGid]
    : [envelope.minX, envelope.minY, envelope.maxX, envelope.maxY, schoolBuffer, excludeRoadGid];

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
  options = {},
) => {
  const {
    marginMin = GRID_MARGIN_MIN,
    marginMax = GRID_MARGIN_MAX,
    maxObstacleCount = MAX_GRID_OBSTACLES,
    plannerMode = 'grid_fallback_shortest_route',
  } = options;
  const directDistance = distance(candidate.startPoint, candidate.endPoint);
  const margin = Math.max(marginMin, Math.min(marginMax, directDistance * 0.8));
  const envelope = getEnvelope([candidate.startPoint, candidate.endPoint], margin);
  const width = envelope.maxX - envelope.minX;
  const height = envelope.maxY - envelope.minY;

  const maxDimension = directDistance >= LONG_ROUTE_DISTANCE_THRESHOLD
    ? LONG_ROUTE_GRID_MAX_DIMENSION
    : GRID_MAX_DIMENSION;

  let cellSize = directDistance >= LONG_ROUTE_DISTANCE_THRESHOLD
    ? LONG_ROUTE_CELL_SIZE
    : GRID_CELL_SIZE;

  while ((width / cellSize > maxDimension || height / cellSize > maxDimension) && cellSize < 300) {
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
    candidate.road_gid,
  );

  if (obstacles.length === 0 || obstacles.length > maxObstacleCount) {
    return null;
  }

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
      const cost = calculatePathCost(smoothed, Number(measured.length_m));
      return {
        connected: true,
        roadGid: candidate.road_gid,
        pathLength: Number(measured.length_m),
        pathCost: cost.pathCost,
        geometry: measured.geometry,
        geometryWkt: lineWkt(smoothed),
        entryPoint: { type: 'Point', coordinates: candidate.startPoint },
        connectionPoint: { type: 'Point', coordinates: candidate.endPoint },
        entryPointWkt: candidate.start_pt_wkt,
        connectionPointWkt: candidate.end_pt_wkt,
        isCurved: smoothed.length > 2,
        turnCount: cost.turnCount,
        acuteTurnCount: cost.acuteTurnCount,
        plannerMode,
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

const solveWithRiverBypassGrid = async (
  db,
  miningGid,
  schoolBuffer,
  candidate,
  obstacleCacheTable,
  schoolSources,
) => solveWithGridFallback(
  db,
  miningGid,
  schoolBuffer,
  candidate,
  obstacleCacheTable,
  schoolSources,
  {
    marginMin: RIVER_GRID_MARGIN_MIN,
    marginMax: RIVER_GRID_MARGIN_MAX,
    maxObstacleCount: MAX_GRID_OBSTACLES * 2,
    plannerMode: 'river_end_grid_bypass',
  },
);

const assessSegmentObstacle = async (
  db,
  schoolSources,
  miningGid,
  schoolBuffer,
  segmentPoints,
  obstacleCacheTable = null,
  excludeRoadGid = null,
) => {
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
      AND NOT (obstacle_type = 'road_barrier' AND obstacle_gid = COALESCE($3, -1))
    ORDER BY intersection_length DESC NULLS LAST, intersection_area DESC NULLS LAST, obstacle_type
    LIMIT 1
  `
    : `
    WITH route AS (
      SELECT ST_GeomFromText($1, 32644) AS route_geom
    ),
    obstacles AS (
      ${buildObstacleUnionSql(schoolSources, miningGid, '$2', '$3')}
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
    obstacleCacheTable ? [segmentWkt, miningGid, excludeRoadGid] : [segmentWkt, schoolBuffer, excludeRoadGid],
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
  excludeRoadGid = null,
) => {
  const obstacleUnionSql = obstacleCacheTable
    ? `
      SELECT obstacle_type, obstacle_gid, obstacle_geom
      FROM ${obstacleCacheTable}
      WHERE NOT (obstacle_type = 'mining_site' AND obstacle_gid = ${miningGid})
        AND NOT (obstacle_type = 'road_barrier' AND obstacle_gid = COALESCE(${excludeRoadGid ?? 'NULL'}, -1))
    `
    : buildObstacleUnionSql(schoolSources, miningGid, '$5', excludeRoadGid == null ? 'NULL' : String(excludeRoadGid));
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
    ORDER BY
      length_m,
      CASE WHEN is_curved THEN 0 ELSE 1 END
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
      ], obstacleCacheTable, candidate.road_gid);

      if (obstacle) {
        blockingObstacle = obstacle;
        blockedSegmentIndex = segmentIndex;
        break;
      }
    }

    if (!blockingObstacle) {
      const optimizedPoints = await optimizePathShape(
        pool,
        miningGid,
        schoolBuffer,
        points,
        obstacleCacheTable,
        schoolSources,
        GRID_MARGIN_MIN,
        candidate.road_gid,
      );
      const measured = await measurePath(pool, optimizedPoints);
      const cost = calculatePathCost(optimizedPoints, Number(measured.length_m));
      return {
        connected: true,
        roadGid: candidate.road_gid,
        pathLength: Number(measured.length_m),
        pathCost: cost.pathCost,
        geometry: measured.geometry,
        geometryWkt: lineWkt(optimizedPoints),
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
        isCurved: isCurved || optimizedPoints.length > 2,
        turnCount: cost.turnCount,
        acuteTurnCount: cost.acuteTurnCount,
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
              : blockingObstacle.obstacle_type === 'road_barrier'
                ? 'ROAD_CROSSING_BLOCK'
              : 'MINING_SITE_BLOCK',
        reasonDetail:
          blockingObstacle.obstacle_type === 'river'
            ? 'The route still intersects a river polygon after detour attempts.'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'The route still intersects the buffered school exclusion area after detour attempts.'
              : blockingObstacle.obstacle_type === 'road_barrier'
                ? 'The route still crosses another road before reaching the target road.'
              : 'The route still intersects other mining polygons after detour attempts.',
      };
    }

    const clearance =
      blockingObstacle.obstacle_type === 'school_buffer'
        ? Math.max(DETOUR_CLEARANCE, schoolBuffer * 0.2)
        : blockingObstacle.obstacle_type === 'river'
          ? Math.max(DETOUR_CLEARANCE, 60)
          : blockingObstacle.obstacle_type === 'road_barrier'
            ? Math.max(DETOUR_CLEARANCE, ROAD_BARRIER_BUFFER * 3)
          : DETOUR_CLEARANCE;

    if (blockingObstacle.obstacle_type === 'river') {
      const riverGridOutcome = await solveWithRiverBypassGrid(
        pool,
        miningGid,
        schoolBuffer,
        candidate,
        obstacleCacheTable,
        schoolSources,
      );

      if (riverGridOutcome) {
        return riverGridOutcome;
      }
    }

      const detour = await createDetourPath(
        pool,
        schoolSources,
        miningGid,
        schoolBuffer,
        [points[blockedSegmentIndex], points[blockedSegmentIndex + 1]],
        blockingObstacle,
        clearance,
        obstacleCacheTable,
        candidate.road_gid,
      );

    if (!detour) {
      return {
        connected: false,
        reasonCode:
          blockingObstacle.obstacle_type === 'river'
            ? 'RIVER_BARRIER'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'SCHOOL_BUFFER_BLOCK'
              : blockingObstacle.obstacle_type === 'road_barrier'
                ? 'ROAD_CROSSING_BLOCK'
              : 'MINING_SITE_BLOCK',
        reasonDetail:
          blockingObstacle.obstacle_type === 'river'
            ? 'No valid river-end detour candidate was found for the blocked segment.'
            : blockingObstacle.obstacle_type === 'school_buffer'
              ? 'No valid curved bypass was found around the buffered school obstacle.'
              : blockingObstacle.obstacle_type === 'road_barrier'
                ? 'No valid route was found that avoids crossing another road before the connection point.'
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

const findShortestDirectCandidateRoute = async (
  pool,
  schoolSources,
  miningGid,
  schoolBuffer,
  obstacleCacheTable = null,
) => {
  const limit = 8;
  const obstacleSql = obstacleCacheTable
    ? `
      SELECT 1
      FROM ${obstacleCacheTable} obstacle_union
      WHERE ST_Intersects(
        obstacle_union.obstacle_geom,
        ST_MakeLine(start_pt, end_pt)
      )
      AND NOT (obstacle_union.obstacle_type = 'mining_site' AND obstacle_union.obstacle_gid = $3)
      AND NOT (obstacle_union.obstacle_type = 'road_barrier' AND obstacle_union.obstacle_gid = road_gid)
    `
    : `
      SELECT 1
      FROM (${buildObstacleUnionSql(schoolSources, miningGid, '$3', 'road_gid')}) obstacle_union
      WHERE ST_Intersects(
        obstacle_union.obstacle_geom,
        ST_MakeLine(start_pt, end_pt)
      )
    `;

  const sql = `
    WITH site AS (
      SELECT gid, geom, ST_Boundary(geom) AS boundary_geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid = $1
        AND ${buildMiningSiteWhereClause()}
    ),
    ranked_roads AS (
      SELECT
        rn.gid AS road_gid,
        rn.road_type,
        ST_ClosestPoint(s.boundary_geom, rn.geom) AS start_pt,
        ST_ClosestPoint(rn.geom, ST_ClosestPoint(s.boundary_geom, rn.geom)) AS end_pt,
        ST_Distance(rn.geom, s.boundary_geom) AS boundary_distance,
        ROW_NUMBER() OVER (
          ORDER BY ST_Distance(rn.geom, s.boundary_geom), rn.geom <-> s.boundary_geom, rn.gid
        ) AS road_rank
      FROM road_network rn
      CROSS JOIN site s
      WHERE rn.geom IS NOT NULL
        AND rn.source_mining_site IS NULL
    ),
    candidate_roads AS (
      SELECT *
      FROM ranked_roads
      WHERE road_rank <= $2
    ),
    visible_candidates AS (
      SELECT
        road_gid,
        road_type,
        start_pt,
        end_pt,
        ST_AsText(start_pt) AS start_pt_wkt,
        ST_AsText(end_pt) AS end_pt_wkt,
        ST_Length(ST_MakeLine(start_pt, end_pt)) AS direct_length_m,
        ST_AsGeoJSON(ST_Transform(ST_MakeLine(start_pt, end_pt), 4326))::jsonb AS geometry
      FROM candidate_roads
      WHERE NOT EXISTS (
        ${obstacleSql}
      )
    )
    SELECT *
    FROM visible_candidates
    ORDER BY direct_length_m, road_gid
    LIMIT 1
  `;

  const params = obstacleCacheTable
    ? [miningGid, limit, miningGid]
    : [miningGid, limit, schoolBuffer];
  const result = await pool.query(sql, params);
  const row = result.rows[0];

  if (!row) {
    return null;
  }

  const route = {
    connected: true,
    roadGid: row.road_gid,
    pathLength: Number(row.direct_length_m),
    pathCost: Number(row.direct_length_m),
    geometry: row.geometry,
    geometryWkt: lineWkt([parsePointWkt(row.start_pt_wkt), parsePointWkt(row.end_pt_wkt)]),
    entryPoint: {
      type: 'Point',
      coordinates: parsePointWkt(row.start_pt_wkt),
    },
    connectionPoint: {
      type: 'Point',
      coordinates: parsePointWkt(row.end_pt_wkt),
    },
    entryPointWkt: row.start_pt_wkt,
    connectionPointWkt: row.end_pt_wkt,
    isCurved: false,
    plannerMode: 'direct_visible_shortest_route',
  };

  return isRouteQualityAcceptable(route) ? route : null;
};

const normalizeMiningGids = (miningGids) =>
  [...new Set((Array.isArray(miningGids) ? miningGids : [])
    .map((value) => Number(value))
    .filter((value) => Number.isInteger(value) && value > 0))];

const validateRouteAgainstMiningSites = async (pool, miningGid, geometryWkt) => {
  const sql = `
    WITH route AS (
      SELECT ST_GeomFromText($2, 32644) AS geom
    ),
    source_site AS (
      SELECT gid, geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid = $1
        AND ${buildMiningSiteWhereClause()}
    ),
    other_sites AS (
      SELECT gid, geom
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE gid <> $1
        AND ${buildMiningSiteWhereClause()}
      UNION ALL
      SELECT NULL::integer AS gid, geom
      FROM ${qualifiedTable(TABLES.forbiddenMining)}
      WHERE geom IS NOT NULL
    )
    SELECT
      EXISTS (
        SELECT 1
        FROM route r
        JOIN source_site s ON true
        WHERE ST_Intersects(r.geom, s.geom)
          AND NOT ST_Touches(r.geom, s.geom)
      ) AS crosses_source_interior,
      EXISTS (
        SELECT 1
        FROM route r
        JOIN other_sites s ON true
        WHERE ST_Intersects(r.geom, s.geom)
          AND NOT ST_Touches(r.geom, s.geom)
      ) AS crosses_other_site,
      EXISTS (
        SELECT 1
        FROM route r
        JOIN source_site s ON true
        WHERE NOT ST_DWithin(ST_StartPoint(r.geom), ST_Boundary(s.geom), 1.0)
      ) AS start_not_on_boundary
  `;

  const result = await pool.query(sql, [miningGid, geometryWkt]);
  const row = result.rows[0];

  if (row?.crosses_other_site) {
    return {
      valid: false,
      reasonCode: 'MINING_SITE_BLOCK',
      reasonDetail: 'The generated road would cross another mining site, so it was rejected.',
    };
  }

  if (row?.crosses_source_interior || row?.start_not_on_boundary) {
    return {
      valid: false,
      reasonCode: 'SOURCE_BOUNDARY_INVALID',
      reasonDetail: 'The generated road must start at the outer boundary of the mining site and stay outside the site polygon.',
    };
  }

  return { valid: true };
};

const removeRoutesForMiningSitesInternal = async (pool, miningGids) => {
  const gids = normalizeMiningGids(miningGids);
  if (gids.length === 0) {
    return { removedRoads: 0, removedStatuses: 0 };
  }

  const statusResult = await pool.query(
    `DELETE FROM mining_connection_status
     WHERE mining_gid = ANY($1::int[])
     RETURNING mining_gid`,
    [gids],
  );

  const roadResult = await pool.query(
    `DELETE FROM road_network
     WHERE source_mining_site = ANY($1::int[])
     RETURNING gid`,
    [gids],
  );

  return {
    removedRoads: roadResult.rowCount,
    removedStatuses: statusResult.rowCount,
  };
};

const persistConnectedRoute = async (pool, miningGid, schoolBuffer, route) => {
  await removeRoutesForMiningSitesInternal(pool, [miningGid]);

  const generatedRoadGid = await insertGeneratedRoad(pool, miningGid, route);
  await upsertConnectionStatus(pool, {
    miningGid,
    isConnected: true,
    connectionRoadGid: generatedRoadGid,
    connectionCost: route.pathCost,
    pathLength: route.pathLength,
    pathStrategy: route.plannerMode,
    reasonCode: null,
    reasonDetail: null,
    entryPointWkt: route.entryPointWkt,
    connectionPointWkt: route.connectionPointWkt,
    geometryWkt: route.geometryWkt,
    metadata: {
      schoolBuffer,
      connectedRoadGid: route.roadGid,
      isCurved: route.isCurved,
    },
  });

  return generatedRoadGid;
};

export const calculateRouteForMiningSite = async (
  pool,
  { miningGid, schoolBuffer, persist = false, schoolSources = null, obstacleCacheTable = null },
) => {
  if (!obstacleCacheTable && typeof pool.connect === 'function' && typeof pool.release !== 'function') {
    const client = await pool.connect();
    try {
      const cachedSchoolSources = schoolSources || (await getExistingSchoolSources(client));
      const obstacleSchoolSources = getObstacleSchoolSources(cachedSchoolSources);
      await createObstacleCache(client, obstacleSchoolSources, schoolBuffer);
      return await calculateRouteForMiningSite(client, {
        miningGid,
        schoolBuffer,
        persist,
        schoolSources: obstacleSchoolSources,
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

  const resolvedSchoolSources = getObstacleSchoolSources(
    schoolSources || (await getExistingSchoolSources(pool)),
  );
  let bestBlocked = null;
  const directVisibleRoute = await findShortestDirectCandidateRoute(
    pool,
    resolvedSchoolSources,
    miningGid,
    schoolBuffer,
    obstacleCacheTable,
  );

  if (directVisibleRoute) {
    const routeValidation = await validateRouteAgainstMiningSites(
      pool,
      miningGid,
      directVisibleRoute.geometryWkt,
    );

    if (!routeValidation.valid) {
      bestBlocked = {
        connected: false,
        reasonCode: routeValidation.reasonCode,
        reasonDetail: routeValidation.reasonDetail,
      };
    } else {
      let generatedRoadGid = null;

      if (persist) {
        generatedRoadGid = await persistConnectedRoute(
          pool,
          miningGid,
          schoolBuffer,
          directVisibleRoute,
        );
      }

      return {
        miningGid,
        miningName: miningSite.name,
        connected: true,
        schoolBuffer,
        plannerMode: directVisibleRoute.plannerMode,
        roadGid: directVisibleRoute.roadGid,
        generatedRoadGid,
        pathLength: directVisibleRoute.pathLength,
        pathCost: directVisibleRoute.pathCost,
        geometry: directVisibleRoute.geometry,
        entryPoint: directVisibleRoute.entryPoint,
        connectionPoint: directVisibleRoute.connectionPoint,
        isCurved: directVisibleRoute.isCurved,
      };
    }
  }

  let bestConnected = null;
  const seenRoads = new Set();
  const candidatePool = [];
  const gridTestedRoads = new Set();
  let evaluatedCandidates = 0;

  const evaluateFallbackCandidates = async () => {
    const fallbackCandidates = getFallbackCandidates(
      candidatePool.filter((candidate) => !gridTestedRoads.has(candidate.road_gid)),
    );

    for (const candidate of fallbackCandidates) {
      gridTestedRoads.add(candidate.road_gid);
      const gridOutcome = await solveWithGridFallback(
        pool,
        miningGid,
        schoolBuffer,
        candidate,
        obstacleCacheTable,
        resolvedSchoolSources,
      );

      if (
        gridOutcome
        && isRouteQualityAcceptable(gridOutcome)
      ) {
        const routeValidation = await validateRouteAgainstMiningSites(
          pool,
          miningGid,
          gridOutcome.geometryWkt,
        );

        if (
          routeValidation.valid
          && (
            !bestConnected
            || gridOutcome.pathCost < bestConnected.pathCost
            || (gridOutcome.pathCost === bestConnected.pathCost && gridOutcome.pathLength < bestConnected.pathLength)
          )
        ) {
          bestConnected = gridOutcome;
        } else if (!routeValidation.valid && !bestBlocked) {
          bestBlocked = {
            connected: false,
            reasonCode: routeValidation.reasonCode,
            reasonDetail: routeValidation.reasonDetail,
          };
        }
      }
    }
  };

  for (const limit of [...QUICK_CANDIDATE_LIMIT_STEPS, ...EXTENDED_CANDIDATE_LIMIT_STEPS]) {
    const candidates = await fetchCandidateRoutes(pool, miningGid, limit);
    let remainingCandidateCanBeatBest = false;

    for (const candidate of candidates) {
      if (seenRoads.has(candidate.road_gid)) {
        continue;
      }
      seenRoads.add(candidate.road_gid);
      candidatePool.push(candidate);

      if (bestConnected && Number(candidate.direct_length_m) >= bestConnected.pathLength) {
        continue;
      }

      remainingCandidateCanBeatBest = true;

      if (evaluatedCandidates >= MAX_CANDIDATE_PATH_EVALUATIONS) {
        continue;
      }

      const firstObstacle = await assessSegmentObstacle(
        pool,
        resolvedSchoolSources,
        miningGid,
        schoolBuffer,
        [candidate.startPoint, candidate.endPoint],
        obstacleCacheTable,
      );

      if (shouldSkipBlockedShortCandidate(candidate, firstObstacle)) {
        if (!bestBlocked) {
          bestBlocked = {
            connected: false,
            reasonCode: 'SCHOOL_BUFFER_BLOCK',
            reasonDetail: 'A very short candidate connector was fully blocked by a school buffer and was skipped to avoid a distorted detour.',
          };
        }
        continue;
      }

      evaluatedCandidates += 1;
      const outcome = await solveCandidatePath(
        pool,
        resolvedSchoolSources,
        miningGid,
        schoolBuffer,
        candidate,
        obstacleCacheTable,
      );

      if (
        outcome.connected
        && isRouteQualityAcceptable(outcome)
      ) {
        const routeValidation = await validateRouteAgainstMiningSites(
          pool,
          miningGid,
          outcome.geometryWkt,
        );

        if (
          routeValidation.valid
          && (
            !bestConnected
            || outcome.pathCost < bestConnected.pathCost
            || (outcome.pathCost === bestConnected.pathCost && outcome.pathLength < bestConnected.pathLength)
          )
        ) {
          bestConnected = outcome;
        } else if (!routeValidation.valid && !bestBlocked) {
          bestBlocked = {
            connected: false,
            reasonCode: routeValidation.reasonCode,
            reasonDetail: routeValidation.reasonDetail,
          };
        }
      }

      if ((!outcome.connected || !isRouteQualityAcceptable(outcome)) && !bestBlocked) {
        bestBlocked = outcome;
      }
    }

    if (bestConnected && !remainingCandidateCanBeatBest) {
      break;
    }
  }

  if (!bestConnected) {
    await evaluateFallbackCandidates();
  }

  if (bestConnected) {
    let generatedRoadGid = null;

    if (persist) {
      generatedRoadGid = await persistConnectedRoute(
        pool,
        miningGid,
        schoolBuffer,
        bestConnected,
      );
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
    evaluatedCandidates,
    diagnostics: {
      evaluationLimitReached: evaluatedCandidates >= MAX_CANDIDATE_PATH_EVALUATIONS,
      obstacleSources: resolvedSchoolSources.map((source) => source.tableName),
    },
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
        evaluatedCandidates,
        evaluationLimitReached: evaluatedCandidates >= MAX_CANDIDATE_PATH_EVALUATIONS,
        obstacleSources: resolvedSchoolSources.map((source) => source.tableName),
      },
    });
  }

  return blockedResponse;
};

export const generateRoutesForMiningSites = async (pool, { batchSize, schoolBuffer, appendMode = true }) => {
  return generateRoutesForMiningSitesWithProgress(pool, {
    batchSize,
    schoolBuffer,
    appendMode,
    retryBlocked: false,
  });
};

export const generateRoutesForMiningSitesWithProgress = async (
  pool,
  {
    batchSize,
    schoolBuffer,
    appendMode = true,
    retryBlocked = false,
    onProgress = null,
  },
) => {
  const client = await pool.connect();

  try {
    if (!appendMode) {
      await reseedBaseRoadNetwork(client);
    }
    const schoolSources = getObstacleSchoolSources(await getExistingSchoolSources(client));
    await createObstacleCache(client, schoolSources, schoolBuffer);

    const totalResult = await client.query(`
      SELECT COUNT(*)::int AS count
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE ${buildMiningSiteWhereClause()}
    `);
    const totalSites = totalResult.rows[0].count;
    const requestedSites = batchSize && batchSize > 0
      ? Math.min(batchSize, totalSites)
      : Math.min(DEFAULT_BATCH_SELECTION, totalSites);
    const limit = Math.min(requestedSites, MAX_BATCH_SELECTION);
    const selectionWasCapped = requestedSites > MAX_BATCH_SELECTION;
    const blockedResult = await client.query(`
      SELECT COUNT(*)::int AS count
      FROM mining_connection_status
      WHERE NOT is_connected
    `);
    const blockedCount = blockedResult.rows[0].count;
    const blockedQuota = retryBlocked && limit > 1 && blockedCount > 0 ? Math.max(1, Math.floor(limit * 0.25)) : 0;
    const pendingQuota = Math.max(1, limit - blockedQuota);

    const miningResult = await client.query(
      `WITH unprocessed AS (
         SELECT gb.gid, 0 AS priority
         FROM ${qualifiedTable(TABLES.miningSites)} gb
         LEFT JOIN mining_connection_status mcs
           ON mcs.mining_gid = gb.gid
         LEFT JOIN LATERAL (
           SELECT rn.geom <-> gb.geom AS road_distance
           FROM road_network rn
           WHERE rn.geom IS NOT NULL
           ORDER BY rn.geom <-> gb.geom
           LIMIT 1
         ) nearest ON true
         WHERE gb.geom IS NOT NULL
           AND ${buildMiningSiteWhereClause('gb')}
           AND mcs.mining_gid IS NULL
         ORDER BY nearest.road_distance NULLS LAST, gb.gid
         LIMIT $1
       ),
       blocked_retry AS (
         SELECT gb.gid, 1 AS priority
         FROM ${qualifiedTable(TABLES.miningSites)} gb
         JOIN mining_connection_status mcs
           ON mcs.mining_gid = gb.gid
         LEFT JOIN LATERAL (
           SELECT rn.geom <-> gb.geom AS road_distance
           FROM road_network rn
           WHERE rn.geom IS NOT NULL
           ORDER BY rn.geom <-> gb.geom
           LIMIT 1
         ) nearest ON true
         WHERE gb.geom IS NOT NULL
           AND ${buildMiningSiteWhereClause('gb')}
           AND NOT mcs.is_connected
         ORDER BY nearest.road_distance NULLS LAST, gb.gid
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

    const totalSelected = miningResult.rows.length;
    let processedCount = 0;
    let totalRoadLength = 0;
    const failedDetails = [];

    if (onProgress) {
      await onProgress({
        stage: 'started',
        totalSites,
        requestedSites,
        selectedSites: limit,
        queuedSites: totalSelected,
        processedSites: 0,
        connectedSites: 0,
        failedSites: 0,
        percentComplete: totalSelected === 0 ? 100 : 0,
        maximumBatchSize: MAX_BATCH_SELECTION,
        defaultBatchSize: DEFAULT_BATCH_SELECTION,
        selectionWasCapped,
      });
    }

    for (let index = 0; index < miningResult.rows.length; index += 1) {
      const row = miningResult.rows[index];
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

      if (onProgress) {
        const finishedSites = index + 1;
        await onProgress({
          stage: finishedSites === totalSelected ? 'completed' : 'running',
          totalSites,
          requestedSites,
          selectedSites: limit,
          queuedSites: totalSelected,
          processedSites: finishedSites,
          connectedSites: processedCount,
          failedSites: failedDetails.length,
          currentMiningGid: row.gid,
          percentComplete: totalSelected === 0 ? 100 : Math.round((finishedSites / totalSelected) * 100),
          maximumBatchSize: MAX_BATCH_SELECTION,
          selectionWasCapped,
        });
      }
    }

    const summary = {
      success: failedDetails.length === 0,
      message:
        selectionWasCapped
          ? `Requested ${requestedSites} sites. Processing is capped at ${MAX_BATCH_SELECTION} sites per request to avoid timeout.`
          : failedDetails.length === 0
            ? 'All selected mining sites received an obstacle-aware connector.'
            : 'Some sites are still blocked after curved detour attempts.',
      plannerMode: PLANNER_MODE,
      processedCount,
      totalRoadLength,
      failedCount: failedDetails.length,
      failedDetails,
      totalSites,
      requestedSites,
      selectedSites: limit,
      maximumBatchSize: MAX_BATCH_SELECTION,
      defaultBatchSize: DEFAULT_BATCH_SELECTION,
      retryBlocked,
      selectionWasCapped,
      schoolBuffer,
      appendMode,
    };

    if (onProgress) {
      await onProgress({
        stage: 'completed',
        totalSites,
        requestedSites,
        selectedSites: limit,
        queuedSites: totalSelected,
        processedSites: totalSelected,
        connectedSites: processedCount,
        failedSites: failedDetails.length,
        percentComplete: 100,
        maximumBatchSize: MAX_BATCH_SELECTION,
        defaultBatchSize: DEFAULT_BATCH_SELECTION,
        selectionWasCapped,
        message: summary.message,
      });
    }

    return summary;
  } finally {
    client.release();
  }
};

export const generateRoutesForSelectedMiningSites = async (
  pool,
  { miningGids, schoolBuffer, replaceExisting = true },
) => {
  const gids = normalizeMiningGids(miningGids);
  if (gids.length === 0) {
    return {
      success: false,
      message: 'Select at least one mining site.',
      processedCount: 0,
      failedCount: 0,
      failedDetails: [],
      selectedSites: 0,
      schoolBuffer,
    };
  }

  const client = await pool.connect();

  try {
    const schoolSources = getObstacleSchoolSources(await getExistingSchoolSources(client));
    await createObstacleCache(client, schoolSources, schoolBuffer);

    if (replaceExisting) {
      await removeRoutesForMiningSitesInternal(client, gids);
    }

    let processedCount = 0;
    const failedDetails = [];

    for (const gid of gids) {
      const outcome = await calculateRouteForMiningSite(client, {
        miningGid: gid,
        schoolBuffer,
        persist: true,
        schoolSources,
        obstacleCacheTable: TEMP_OBSTACLE_CACHE,
      });

      if (!outcome) {
        failedDetails.push({
          gid,
          reason: 'Mining site not found during processing.',
        });
        continue;
      }

      if (outcome.connected) {
        processedCount += 1;
      } else {
        failedDetails.push({
          gid,
          reason: outcome.reasonDetail,
          code: outcome.reasonCode,
        });
      }
    }

    return {
      success: failedDetails.length === 0,
      message:
        failedDetails.length === 0
          ? `Connectivity generated for ${processedCount} selected site(s).`
          : 'Some selected sites are still blocked after obstacle-aware routing.',
      plannerMode: PLANNER_MODE,
      processedCount,
      failedCount: failedDetails.length,
      failedDetails,
      selectedSites: gids.length,
      schoolBuffer,
      selectedMiningGids: gids,
    };
  } finally {
    client.release();
  }
};

export const removeRoutesForMiningSites = async (pool, { miningGids }) => {
  const gids = normalizeMiningGids(miningGids);
  if (gids.length === 0) {
    return {
      success: false,
      message: 'Select at least one mining site.',
      removedRoads: 0,
      removedStatuses: 0,
      selectedSites: 0,
    };
  }

  const result = await removeRoutesForMiningSitesInternal(pool, gids);
  return {
    success: true,
    message: `Removed generated connectivity for ${gids.length} selected site(s).`,
    removedRoads: result.removedRoads,
    removedStatuses: result.removedStatuses,
    selectedSites: gids.length,
    selectedMiningGids: gids,
  };
};

export const resetRoadNetwork = async (pool) => {
  await reseedBaseRoadNetwork(pool);
};
