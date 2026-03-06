import express from 'express';
import cors from 'cors';
import pool from './db.js';

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// Error handler wrapper
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// Health check
app.get('/api/health', asyncHandler(async (req, res) => {
  const result = await pool.query('SELECT NOW()');
  res.json({ status: 'OK', timestamp: result.rows[0].now });
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
        'properties', jsonb_build_object(
          'road_code', tr_rdcode,
          'length_km', length_km
        )
      ) as feature
      FROM national_highway_2018
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get mining sites
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
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Centroid(gb.geom), 4326))::jsonb,
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
        'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
        'properties', jsonb_build_object('name', wetname, 'area', area)
      ) as feature
      FROM uprsac_09xxxx_riverxxxxx_09042018
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get schools
app.get('/api/schools', asyncHandler(async (req, res) => {
  const result = await pool.query(`
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
          'name', schname,
          'district', districtna,
          'category', schcat_des
        )
      ) as feature
      FROM uprsac_09xxxx_educschool_20132016
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get obstacles
app.get('/api/obstacles', asyncHandler(async (req, res) => {
  const { schoolBuffer = 500 } = req.query;
  const bufferDist = parseFloat(schoolBuffer);
  
  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      -- Schools with buffer (transform to 4326 for display)
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(ST_Buffer(geom::geometry, $1), 4326))::jsonb,
        'properties', jsonb_build_object('type', 'school_buffer', 'name', schname)
      ) as feature
      FROM uprsac_09xxxx_educschool_20132016
      WHERE geom IS NOT NULL
      
      UNION ALL
      
      -- Rivers
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
        'properties', jsonb_build_object('type', 'river', 'name', wetname)
      ) as feature
      FROM uprsac_09xxxx_riverxxxxx_09042018
      WHERE geom IS NOT NULL
      
      UNION ALL
      
      -- Mining sites as obstacles
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
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
        'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
        'properties', jsonb_build_object(
          'road_type', road_type,
          'length_km', length_km,
          'cost', cost,
          'source_mining', source_mining_site
        )
      ) as feature
      FROM road_network
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Calculate optimal route for single mining site
app.post('/api/calculate-route', asyncHandler(async (req, res) => {
  const { miningGid, schoolBuffer = 500 } = req.body;
  
  if (!miningGid) {
    return res.status(400).json({ error: 'miningGid is required' });
  }

  // Use geometry distance in meters (32644 is UTM in meters) instead of geography
  const result = await pool.query(`
    WITH mining_site AS (
      SELECT gid, geom, ST_Centroid(geom) as center
      FROM gorakhpur_brickkiln
      WHERE gid = $1
    ),
    nearest_road AS (
      SELECT 
        rn.gid as road_gid,
        rn.geom as road_geom,
        ST_ClosestPoint(rn.geom, ms.center) as connection_point,
        ST_Distance(rn.geom, ms.center) as distance_meters
      FROM road_network rn, mining_site ms
      ORDER BY rn.geom <-> ms.center
      LIMIT 1
    ),
    exit_point AS (
      SELECT 
        ms.gid,
        ms.geom,
        ST_ClosestPoint(ST_Boundary(ms.geom), nr.connection_point) as exit_pt
      FROM mining_site ms, nearest_road nr
    )
    SELECT 
      ms.gid as mining_gid,
      ST_Transform(ep.exit_pt, 4326) as entry_point,
      ST_Transform(nr.connection_point, 4326) as connection_point,
      nr.road_gid,
      ST_AsGeoJSON(ST_Transform(ST_MakeLine(ep.exit_pt, nr.connection_point), 4326))::jsonb as path_geom,
      nr.distance_meters as path_length,
      nr.distance_meters as path_cost,
      true as connected_to_existing
    FROM mining_site ms
    JOIN exit_point ep ON ms.gid = ep.gid
    CROSS JOIN nearest_road nr
  `, [miningGid]);
  
  if (result.rows.length === 0) {
    return res.status(404).json({ error: 'Mining site not found' });
  }
  
  const row = result.rows[0];
  
  res.json({
    miningGid: row.mining_gid,
    entryPoint: row.entry_point,
    connectionPoint: row.connection_point,
    roadGid: row.road_gid,
    pathLength: parseFloat(row.path_length),
    pathCost: parseFloat(row.path_cost),
    connectedToExisting: row.connected_to_existing,
    geometry: row.path_geom
  });
}));

// Generate all roads batch process - FIXED (no geography, use geometry in meters)
// Generate all roads batch process - FIXED with explicit casting
app.post('/api/generate-all-roads', asyncHandler(async (req, res) => {
    const { batchSize = 10 } = req.body;
    
    // Clear existing
    await pool.query('TRUNCATE mining_connection_status');
    await pool.query("DELETE FROM road_network WHERE road_type = 'mining_access'");
    
    // Get mining sites
    const miningSites = await pool.query(`
      SELECT gid, geom, ST_Centroid(geom) as center
      FROM gorakhpur_brickkiln
      WHERE geom IS NOT NULL
      ORDER BY gid
      LIMIT $1
    `, [parseInt(batchSize)]);
    
    let processed = 0;
    let totalLength = 0;
    const failedSites = [];
    
    // Process each site individually
    for (const site of miningSites.rows) {
      try {
        // Find nearest road using geometry distance (meters in UTM)
        const nearestRoad = await pool.query(`
          SELECT 
            rn.gid as road_gid,
            ST_ClosestPoint(rn.geom, $1) as connection_pt,
            ST_Distance(rn.geom, $1) as distance_meters
          FROM road_network rn
          ORDER BY rn.geom <-> $1
          LIMIT 1
        `, [site.center]);
        
        if (nearestRoad.rows.length === 0) {
          failedSites.push(site.gid);
          continue;
        }
        
        const road = nearestRoad.rows[0];
        
        // Calculate exit point on mining boundary
        const exitPointResult = await pool.query(`
          SELECT ST_ClosestPoint(ST_Boundary($1), $2) as exit_pt
        `, [site.geom, road.connection_pt]);
        
        const exit_pt = exitPointResult.rows[0].exit_pt;
        
        // Calculate path geometry and length
        const pathResult = await pool.query(`
          SELECT 
            ST_MakeLine($1, $2) as path_geom,
            ST_Distance($1, $2) as path_length
        `, [exit_pt, road.connection_pt]);
        
        const { path_geom, path_length } = pathResult.rows[0];
        // Explicitly convert to number to ensure proper type
        const pathLengthNum = Math.round(parseFloat(path_length) * 100) / 100; // Round to 2 decimals
        
        // Insert connection status with explicit casting
        await pool.query(`
          INSERT INTO mining_connection_status (
            mining_gid, is_connected, connection_road_gid, 
            connection_cost, connected_at, entry_point_geom, path_length
          ) VALUES ($1::integer, true, $2::integer, $3::numeric, NOW(), $4, $5::numeric)
        `, [site.gid, road.road_gid, pathLengthNum, exit_pt, pathLengthNum]);
        
        // Insert new road into network
        await pool.query(`
          INSERT INTO road_network (
            road_type, source_mining_site, length_km, cost, reverse_cost, geom
          ) VALUES (
            'mining_access',
            $1::integer,
            $2::numeric / 1000,
            $2::numeric,
            $2::numeric,
            $3
          )
        `, [site.gid, pathLengthNum, path_geom]);
        
        processed++;
        totalLength += pathLengthNum;
        
      } catch (err) {
        console.error(`Error processing mining site ${site.gid}:`, err);
        failedSites.push(site.gid);
      }
    }
    
    res.json({
      processedCount: processed,
      totalRoadLength: totalLength,
      failedSites: failedSites,
      success: failedSites.length === 0
    });
  }));

// Add new road to network
app.post('/api/add-road', asyncHandler(async (req, res) => {
  const { roadType, sourceMiningSite, geometry, lengthKm, cost } = req.body;
  
  // Convert from GeoJSON (4326) to 32644 for storage
  const result = await pool.query(`
    INSERT INTO road_network (
      road_type, source_mining_site, length_km, cost, reverse_cost, geom
    ) VALUES ($1, $2, $3, $4, $4, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($5), 4326), 32644))
    RETURNING gid
  `, [roadType, sourceMiningSite, lengthKm, cost, JSON.stringify(geometry)]);
  
  res.json({ success: true, roadGid: result.rows[0].gid });
}));

// Reset network
app.post('/api/reset-network', asyncHandler(async (req, res) => {
  await pool.query('TRUNCATE road_network');
  await pool.query('TRUNCATE mining_connection_status');
  
  // Re-insert highways (convert to 32644)
  await pool.query(`
    INSERT INTO road_network (road_type, length_km, cost, reverse_cost, geom)
    SELECT 
      'highway',
      length_km,
      COALESCE(cost, length_km::double precision),
      COALESCE(reverse_cost, length_km::double precision),
      ST_GeometryN(geom, 1)::geometry(LineString, 32644)
    FROM national_highway_2018
    WHERE geom IS NOT NULL
  `);
  
  res.json({ success: true, message: 'Network reset to highways only' });
}));

// Get statistics
app.get('/api/statistics', asyncHandler(async (req, res) => {
  const result = await pool.query(`
    SELECT 
      (SELECT COUNT(*) FROM gorakhpur_brickkiln) as total_mining_sites,
      (SELECT COUNT(*) FROM mining_connection_status WHERE is_connected) as connected_sites,
      (SELECT COALESCE(SUM(path_length), 0) FROM mining_connection_status WHERE is_connected) as total_road_length,
      (SELECT COUNT(*) FROM road_network WHERE road_type = 'mining_access') as new_roads_count,
      (SELECT COALESCE(SUM(length_km), 0) FROM road_network WHERE road_type = 'mining_access') as new_roads_length
  `);
  
  res.json(result.rows[0]);
}));

// Global error handler
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ 
    error: err.message,
    detail: err.detail || 'Internal server error'
  });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Test: http://localhost:${PORT}/api/health`);
});