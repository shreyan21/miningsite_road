import express from 'express';
import cors from 'cors';
import pool from './db.js';

const app = express();
const PORT = process.env.PORT || 5000;

const processingStatus = new Map();
let jobCounter = 0;

app.use(cors());
app.use(express.json());

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// =====================================================
// MULTI-TABLE SCHOOL CONFIGURATION
// =====================================================
const SCHOOL_TABLES = [
  { 
    table: 'uprsac_09xxxx_educschool_20132016', 
    nameColumn: 'schname', 
    districtColumn: 'districtna',
    sourceLabel: 'UPRSAC Schools'
  },
  { 
    table: 'gorakhpur_ps',
    nameColumn: 'field1',
    districtColumn: 'field3',
    sourceLabel: 'Gorakhpur PS'
  }
];

// Health check
app.get('/api/health', asyncHandler(async (req, res) => {
  const result = await pool.query('SELECT NOW()');
  res.json({ 
    status: 'OK', 
    timestamp: result.rows[0].now,
    schoolTables: SCHOOL_TABLES.map(t => t.table),
    version: '2.7-knn-fixed'
  });
}));

// DEBUG: Check gorakhpur_ps sample data
app.get('/api/debug/gorakhpur-ps', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT gid, field1, field2, field3, field4, field5, field6, field7, field8, 
           ST_AsText(geom) as geom_wkt,
           ST_SRID(geom) as srid
    FROM gorakhpur_ps
    WHERE geom IS NOT NULL
    LIMIT 5
  `);

  const countResult = await pool.query(`
    SELECT COUNT(*) as total, COUNT(geom) as with_geom 
    FROM gorakhpur_ps
  `);

  res.json({
    sample_data: result.rows,
    counts: countResult.rows[0],
    columns: ['gid', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'geom']
  });
}));

// Get all highways
app.get('/api/highways', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      SELECT jsonb_build_object(
        'type', 'Feature',
        'id', gid,
        'geometry', ST_AsGeoJSON(ST_Transform(ST_GeometryN(geom, 1), 4326))::jsonb,
        'properties', jsonb_build_object('road_code', tr_rdcode, 'length_km', length_km)
      ) as feature
      FROM national_highway_2018
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get mining sites as polygons
app.get('/api/mining-sites', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      SELECT jsonb_build_object(
        'type', 'Feature',
        'id', gb.gid,
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Force2D(gb.geom), 4326))::jsonb,
        'properties', jsonb_build_object(
          'name', gb.name,
          'district', gb.district,
          'is_connected', COALESCE(mcs.is_connected, false),
          'connection_cost', mcs.connection_cost
        )
      ) as feature
      FROM gorakhpur_brickkiln gb
      LEFT JOIN mining_connection_status mcs ON gb.gid = mcs.mining_gid
      WHERE gb.geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get rivers
app.get('/api/rivers', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      SELECT jsonb_build_object(
        'type', 'Feature',
        'id', gid,
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Force2D(geom), 4326))::jsonb,
        'properties', jsonb_build_object('name', wetname, 'area', area)
      ) as feature
      FROM uprsac_09xxxx_riverxxxxx_09042018
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get schools - FIXED to handle schoolType parameter with proper column names
app.get('/api/schools', asyncHandler(async (req, res) => {
  const { schoolType } = req.query;

  console.log('Schools endpoint called with schoolType:', schoolType);

  let query;
  let params = [];

  if (schoolType === 'gorakhpur_ps') {
    // Query specifically from gorakhpur_ps table
    query = `
      SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
      ) as geojson
      FROM (
        SELECT jsonb_build_object(
          'type', 'Feature',
          'id', gid,
          'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
          'properties', jsonb_build_object(
            'gid', gid,
            'name', COALESCE(NULLIF(field1, ''), NULLIF(field2, ''), NULLIF(field3, ''), 'School ' || gid),
            'field1', field1,
            'field2', field2,
            'field3', field3,
            'school_type', 'gorakhpur_ps'
          )
        ) as feature
        FROM gorakhpur_ps
        WHERE geom IS NOT NULL
      ) features;
    `;
  } else {
    // Default to uprsac schools
    query = `
      SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
      ) as geojson
      FROM (
        SELECT jsonb_build_object(
          'type', 'Feature',
          'id', gid,
          'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
          'properties', jsonb_build_object(
            'gid', gid,
            'name', COALESCE(schname, 'School ' || gid)
          )
        ) as feature
        FROM uprsac_09xxxx_educschool_20132016
        WHERE geom IS NOT NULL
      ) features;
    `;
  }

  try {
    const result = await pool.query(query, params);
    const geojson = result.rows[0].geojson;

    const featureCount = geojson.features ? geojson.features.length : 0;
    console.log(`Returning ${featureCount} schools for type: ${schoolType || 'default'}`);

    if (featureCount > 0 && schoolType === 'gorakhpur_ps') {
      console.log('Sample school:', JSON.stringify(geojson.features[0].properties));
    }

    res.json(geojson);
  } catch (err) {
    console.error('Error in schools query:', err.message);
    console.error('Query:', query);
    res.status(500).json({ error: err.message, query: query });
  }
}));

// Get obstacles with MULTI-TABLE school buffer
app.get('/api/obstacles', asyncHandler(async (req, res) => {
  const { schoolBuffer = 500 } = req.query;
  const bufferDist = parseFloat(schoolBuffer);

  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      -- School buffers from gorakhpur_ps
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Buffer(geom::geometry, $1), 4326))::jsonb,
        'properties', jsonb_build_object(
          'type', 'school_buffer',
          'name', COALESCE(NULLIF(field1, ''), NULLIF(field2, ''), NULLIF(field3, ''), 'School'),
          'source_table', 'gorakhpur_ps'
        )
      ) as feature
      FROM gorakhpur_ps
      WHERE geom IS NOT NULL

      UNION ALL

      -- School buffers from uprsac schools
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Buffer(geom::geometry, $1), 4326))::jsonb,
        'properties', jsonb_build_object(
          'type', 'school_buffer',
          'name', COALESCE(schname, 'School'),
          'source_table', 'uprsac_09xxxx_educschool_20132016'
        )
      ) as feature
      FROM uprsac_09xxxx_educschool_20132016
      WHERE geom IS NOT NULL

      UNION ALL

      -- Rivers
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Force2D(geom), 4326))::jsonb,
        'properties', jsonb_build_object('type', 'river', 'name', wetname)
      ) as feature
      FROM uprsac_09xxxx_riverxxxxx_09042018
      WHERE geom IS NOT NULL

      UNION ALL

      -- Mining sites as obstacles
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Force2D(geom), 4326))::jsonb,
        'properties', jsonb_build_object('type', 'mining_site', 'name', name)
      ) as feature
      FROM gorakhpur_brickkiln
      WHERE geom IS NOT NULL
    ) features
  `, [bufferDist]);

  res.json(result.rows[0].geojson);
}));

// Get current road network
app.get('/api/roads', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      SELECT jsonb_build_object(
        'type', 'Feature',
        'id', gid,
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Force2D(geom), 4326))::jsonb,
        'properties', jsonb_build_object(
          'road_type', road_type,
          'length_km', length_km,
          'cost', cost,
          'source_mining', source_mining_site,
          'is_curved', COALESCE(is_curved, false)
        )
      ) as feature
      FROM road_network
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// BACKGROUND PROCESSING: Start road generation job
app.post('/api/generate-all-roads', asyncHandler(async (req, res) => {
  const { batchSize = null, schoolBuffer = 500 } = req.body;

  const jobId = ++jobCounter;
  const jobStatus = {
    id: jobId,
    status: 'starting',
    progress: 0,
    total: 0,
    processed: 0,
    failed: 0,
    message: 'Initializing...',
    result: null,
    error: null,
    startTime: new Date()
  };

  processingStatus.set(jobId, jobStatus);

  // Start async processing
  processRoadsAsync(jobId, batchSize, schoolBuffer);

  res.json({
    jobId: jobId,
    status: 'started',
    message: 'Road generation started in background',
    checkStatus: `/api/job-status/${jobId}`
  });
}));

// Check job status
app.get('/api/job-status/:jobId', asyncHandler(async (req, res) => {
  const jobId = parseInt(req.params.jobId);
  const status = processingStatus.get(jobId);

  if (!status) {
    return res.status(404).json({ error: 'Job not found' });
  }

  res.json(status);
}));

// Cancel job
app.post('/api/job-cancel/:jobId', asyncHandler(async (req, res) => {
  const jobId = parseInt(req.params.jobId);
  const status = processingStatus.get(jobId);

  if (!status) {
    return res.status(404).json({ error: 'Job not found' });
  }

  status.status = 'cancelled';
  status.message = 'Cancelled by user';
  res.json({ success: true, message: 'Job cancelled' });
}));

// =====================================================
// FIXED: Async processing with proper KNN operator usage
// =====================================================
async function processRoadsAsync(jobId, batchSize, schoolBuffer) {
  const status = processingStatus.get(jobId);
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query('TRUNCATE mining_connection_status');
    await client.query("DELETE FROM road_network WHERE road_type = 'mining_access'");

    const countResult = await client.query(`SELECT COUNT(*) as total FROM gorakhpur_brickkiln WHERE geom IS NOT NULL`);
    const totalSites = parseInt(countResult.rows[0].total);
    const limit = batchSize ? parseInt(batchSize) : totalSites;

    status.total = limit;
    status.status = 'processing';
    status.message = `Processing ${limit} sites...`;

    const CHUNK_SIZE = 5;
    let processed = 0;
    let totalLength = 0;
    const failedSites = [];
    const failedDetails = [];

    for (let offset = 0; offset < limit; offset += CHUNK_SIZE) {
      if (status.status === 'cancelled') {
        await client.query('ROLLBACK');
        return;
      }

      const chunkLimit = Math.min(CHUNK_SIZE, limit - offset);

      // Get mining sites for this chunk
      const miningSites = await client.query(`
        SELECT gid, ST_Force2D(geom) as mining_geom, ST_Centroid(ST_Force2D(geom)) as center
        FROM gorakhpur_brickkiln
        WHERE geom IS NOT NULL
        ORDER BY gid
        LIMIT $1 OFFSET $2
      `, [chunkLimit, offset]);

      // Process each site in the chunk
      for (const site of miningSites.rows) {
        try {
          if (status.status === 'cancelled') {
            await client.query('ROLLBACK');
            return;
          }

          // Get obstacles - using explicit geometry casting
          const obstaclesResult = await client.query(`
            WITH mining_obstacles AS (
              SELECT ST_Union(ST_Force2D(geom)) as geom
              FROM gorakhpur_brickkiln
              WHERE geom IS NOT NULL AND gid != $1
            ),
            school_obstacles AS (
              SELECT ST_Union(ST_Buffer(geom::geometry, $2)) as geom
              FROM (
                SELECT geom FROM gorakhpur_ps WHERE geom IS NOT NULL
                UNION ALL
                SELECT geom FROM uprsac_09xxxx_educschool_20132016 WHERE geom IS NOT NULL
              ) all_schools
            )
            SELECT 
              ST_Union(COALESCE(m.geom, 'GEOMETRYCOLLECTION EMPTY'::geometry), 
                       COALESCE(s.geom, 'GEOMETRYCOLLECTION EMPTY'::geometry)) as all_obstacles
            FROM mining_obstacles m
            FULL OUTER JOIN school_obstacles s ON true
          `, [site.gid, schoolBuffer]);

          const allObstacles = obstaclesResult.rows[0]?.all_obstacles;

          // Get boundary points
          const boundaryPointsResult = await client.query(`
            WITH boundary_lines AS (
              SELECT (ST_Dump(ST_Boundary($1::geometry))).geom as line
            ),
            all_points AS (
              SELECT (ST_DumpPoints(line)).geom as pt
              FROM boundary_lines
              WHERE line IS NOT NULL AND ST_GeometryType(line) = 'ST_LineString'

              UNION ALL

              SELECT ST_LineInterpolatePoint(
                line, 
                n / GREATEST(ST_Length(line) / 50.0, 1.0)
              ) as pt
              FROM boundary_lines
              CROSS JOIN generate_series(1, GREATEST(FLOOR(ST_Length(line) / 50.0)::integer - 1, 0)) as n
              WHERE line IS NOT NULL 
                AND ST_GeometryType(line) = 'ST_LineString'
                AND ST_Length(line) > 100
            )
            SELECT DISTINCT pt
            FROM all_points
            WHERE pt IS NOT NULL
            LIMIT 8
          `, [site.mining_geom]);

          const boundaryPoints = boundaryPointsResult.rows.map(r => r.pt);

          if (boundaryPoints.length === 0) {
            failedSites.push(site.gid);
            failedDetails.push({ gid: site.gid, reason: 'No boundary points found' });
            continue;
          }

          let bestConnection = null;
          let bestCost = Infinity;

          // FIXED: Use proper geometry casting for KNN operator
          // The <-> operator needs both sides to be geometry types
          const nearbyRoads = await client.query(`
            SELECT 
              rn.gid as road_gid,
              ST_ClosestPoint(rn.geom::geometry, $1::geometry) as road_point,
              rn.geom::geometry <-> $1::geometry as knn_distance
            FROM road_network rn
            WHERE rn.geom IS NOT NULL
            ORDER BY rn.geom::geometry <-> $1::geometry
            LIMIT 3
          `, [site.center]);

          // Try straight connections first
          for (const road of nearbyRoads.rows) {
            for (const exitPt of boundaryPoints) {
              const pathCheck = await client.query(`
                SELECT 
                  ST_MakeLine($1::geometry, $2::geometry) as path_geom,
                  ST_Distance($1::geometry, $2::geometry) as path_length,
                  ST_Crosses(ST_MakeLine($1::geometry, $2::geometry), $3::geometry) as crosses_own_site,
                  CASE 
                    WHEN $4::geometry IS NULL OR ST_IsEmpty($4::geometry) THEN false
                    ELSE ST_Crosses(ST_MakeLine($1::geometry, $2::geometry), $4::geometry)
                  END as crosses_obstacles
              `, [exitPt, road.road_point, site.mining_geom, allObstacles]);

              const check = pathCheck.rows[0];
              const pathLength = parseFloat(check.path_length);

              if (check.crosses_own_site || check.crosses_obstacles) {
                continue;
              }

              if (pathLength < bestCost) {
                bestCost = pathLength;
                bestConnection = {
                  roadGid: parseInt(road.road_gid),
                  exitPt: exitPt,
                  roadPoint: road.road_point,
                  pathGeom: check.path_geom,
                  pathLength: pathLength,
                  isCurved: false
                };
              }
            }
          }

          // Try curved/angled paths if no straight path found
          if (!bestConnection && boundaryPoints.length >= 2) {
            for (let i = 0; i < Math.min(boundaryPoints.length, 6) && !bestConnection; i++) {
              for (let j = i + 1; j < Math.min(boundaryPoints.length, 6); j++) {
                const pt1 = boundaryPoints[i];
                const pt2 = boundaryPoints[j];

                // FIXED: KNN with proper geometry casting
                const roadRes = await client.query(`
                  SELECT ST_ClosestPoint(rn.geom::geometry, $1::geometry) as road_pt
                  FROM road_network rn
                  WHERE rn.geom IS NOT NULL
                  ORDER BY rn.geom::geometry <-> $1::geometry
                  LIMIT 1
                `, [pt2]);

                if (roadRes.rows.length === 0) continue;
                const roadPt = roadRes.rows[0].road_pt;

                const curvedCheck = await client.query(`
                  SELECT 
                    ST_MakeLine(ARRAY[$1::geometry, $2::geometry, $3::geometry]) as path_geom,
                    ST_Length(ST_MakeLine(ARRAY[$1::geometry, $2::geometry, $3::geometry])) as path_length,
                    ST_Crosses(ST_MakeLine($1::geometry, $2::geometry), $4::geometry) as seg1_crosses_own,
                    ST_Crosses(ST_MakeLine($2::geometry, $3::geometry), $4::geometry) as seg2_crosses_own,
                    CASE 
                      WHEN $5::geometry IS NULL OR ST_IsEmpty($5::geometry) THEN false
                      ELSE ST_Crosses(ST_MakeLine($1::geometry, $2::geometry), $5::geometry) OR
                           ST_Crosses(ST_MakeLine($2::geometry, $3::geometry), $5::geometry)
                    END as crosses_obstacles
                `, [pt1, pt2, roadPt, site.mining_geom, allObstacles]);

                const curved = curvedCheck.rows[0];
                if (curved && !curved.seg1_crosses_own && !curved.seg2_crosses_own && !curved.crosses_obstacles) {
                  const pathLength = parseFloat(curved.path_length);
                  if (pathLength < bestCost) {
                    // FIXED: KNN lookup with proper casting
                    const roadGidRes = await client.query(`
                      SELECT gid FROM road_network rn
                      WHERE rn.geom IS NOT NULL
                      ORDER BY rn.geom::geometry <-> $1::geometry 
                      LIMIT 1
                    `, [pt2]);

                    bestCost = pathLength;
                    bestConnection = {
                      roadGid: parseInt(roadGidRes.rows[0]?.gid) || 1,
                      exitPt: pt1,
                      roadPoint: pt2,
                      pathGeom: curved.path_geom,
                      pathLength: pathLength,
                      isCurved: true
                    };
                  }
                }
              }
            }
          }

          if (!bestConnection) {
            failedSites.push(site.gid);
            failedDetails.push({ gid: site.gid, reason: 'No valid path found (obstacles block all routes)' });
          } else {
            const pathLengthNum = Math.round(bestConnection.pathLength * 100) / 100;
            const siteGid = parseInt(site.gid);
            const roadGid = parseInt(bestConnection.roadGid);

            const path2D = await client.query(`
              SELECT ST_Force2D($1::geometry) as geom_2d
            `, [bestConnection.pathGeom]);

            await client.query(`
              INSERT INTO mining_connection_status (
                mining_gid, is_connected, connection_road_gid, 
                connection_cost, connected_at, entry_point_geom, path_length, is_curved
              ) VALUES ($1::integer, true, $2::integer, $3::numeric, NOW(), 
                        ST_Force2D($4::geometry), $5::numeric, $6::boolean)
            `, [siteGid, roadGid, pathLengthNum, 
                bestConnection.exitPt, pathLengthNum, bestConnection.isCurved]);

            await client.query(`
              INSERT INTO road_network (road_type, source_mining_site, length_km, cost, reverse_cost, geom, is_curved)
              VALUES ('mining_access', $1::integer, $2::numeric / 1000, $2::numeric, $2::numeric, $3, $4::boolean)
            `, [siteGid, pathLengthNum, path2D.rows[0].geom_2d, bestConnection.isCurved]);

            processed++;
            totalLength += pathLengthNum;
          }

        } catch (err) {
          console.error(`Error processing site ${site.gid}:`, err.message);
          failedSites.push(site.gid);
          failedDetails.push({ gid: site.gid, reason: err.message });
        }
      }

      // Update progress after each chunk
      status.processed = processed;
      status.failed = failedSites.length;
      status.progress = Math.round((offset + chunkLimit) / limit * 100);
      status.message = `Processed ${processed} sites (${status.progress}%)...`;
    }

    await client.query('COMMIT');

    status.status = 'completed';
    status.progress = 100;
    status.processed = processed;
    status.failed = failedSites.length;
    status.message = `Completed: ${processed} roads, ${failedSites.length} failed`;
    status.result = {
      processedCount: processed,
      totalRoadLength: totalLength,
      failedSites: failedSites,
      failedDetails: failedDetails,
      totalSites: limit,
      success: failedSites.length === 0
    };

  } catch (err) {
    await client.query('ROLLBACK');
    status.status = 'error';
    status.error = err.message;
    status.message = `Error: ${err.message}`;
    console.error('Job error:', err);
  } finally {
    client.release();
    status.endTime = new Date();
  }
}

// Reset network
app.post('/api/reset-network', asyncHandler(async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('TRUNCATE road_network');
    await client.query('TRUNCATE mining_connection_status');

    await client.query(`
      INSERT INTO road_network (road_type, length_km, cost, reverse_cost, geom)
      SELECT 
        'highway', length_km, length_km::double precision, length_km::double precision,
        ST_Force2D(ST_GeometryN(geom, 1))::geometry(LineString, 32644)
      FROM national_highway_2018 WHERE geom IS NOT NULL
    `);

    await client.query('COMMIT');
    res.json({ success: true, message: 'Network reset' });
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}));

// Get statistics
app.get('/api/statistics', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT 
      (SELECT COUNT(*) FROM gorakhpur_brickkiln) as total_mining_sites,
      (SELECT COUNT(*) FROM mining_connection_status WHERE is_connected) as connected_sites,
      (SELECT COALESCE(SUM(path_length), 0) FROM mining_connection_status WHERE is_connected) as total_road_length,
      (SELECT COUNT(*) FROM road_network WHERE road_type = 'mining_access') as new_roads_count,
      (SELECT COALESCE(SUM(length_km), 0) FROM road_network WHERE road_type = 'mining_access') as new_roads_length,
      (SELECT COUNT(*) FROM mining_connection_status WHERE is_curved = true) as curved_roads_count
  `);
  res.json(result.rows[0]);
}));

// Error handler
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ error: err.message, detail: err.detail || 'Internal error' });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`KNN-FIXED Multi-school obstacle avoidance enabled`);
  console.log(`Configured school tables: ${SCHOOL_TABLES.map(t => t.table).join(', ')}`);
  console.log(`Submit job: POST /api/generate-all-roads`);
  console.log(`Check status: GET /api/job-status/:jobId`);
  console.log(`Fixed: Proper geometry casting for KNN operator <->`);
});