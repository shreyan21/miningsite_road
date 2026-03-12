import express from 'express';
import cors from 'cors';
import pool from './db.js';

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

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
        'geometry', ST_AsGeoJSON(ST_Transform(gb.geom, 4326))::jsonb,
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
        'properties', jsonb_build_object('name', schname, 'district', districtna)
      ) as feature
      FROM uprsac_09xxxx_educschool_20132016
      WHERE geom IS NOT NULL
    ) features
  `);
  res.json(result.rows[0].geojson);
}));

// Get obstacles with school buffer
app.get('/api/obstacles', asyncHandler(async (req, res) => {
  const { schoolBuffer = 500 } = req.query;
  const bufferDist = parseFloat(schoolBuffer);
  
  const result = await pool.query(`
    SELECT jsonb_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(jsonb_agg(feature), '[]'::jsonb)
    ) as geojson
    FROM (
      -- Schools with buffer
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
      
      -- Mining sites as obstacles (for other sites)
      SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
        'properties', jsonb_build_object('type', 'mining_site', 'name', name)
      ) as feature
      FROM gorakhpur_brickkiln
      WHERE geom IS NOT NULL
    ) features
  `, [bufferDist]);
  
  res.json(result.rows.rows[0].geojson);
}));

// Get current road network (all road types)
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

// CRITICAL FIX: Calculate route that ONLY touches mining site boundary
// Road starts at boundary and goes outward, never entering the mining site
app.post('/api/calculate-route', asyncHandler(async (req, res) => {
  const { miningGid, schoolBuffer = 500 } = req.body;
  
  if (!miningGid) return res.status(400).json({ error: 'miningGid is required' });

  const result = await pool.query(`
    WITH mining_site AS (
      SELECT gid, geom as mining_geom, ST_Centroid(geom) as center
      FROM gorakhpur_brickkiln 
      WHERE gid = $1
    ),
    -- Find nearest road to mining site center
    nearest_road AS (
      SELECT 
        rn.gid as road_gid,
        ST_ClosestPoint(rn.geom, ms.center) as road_point,
        rn.geom as road_geom
      FROM road_network rn, mining_site ms
      ORDER BY rn.geom <-> ms.center
      LIMIT 1
    ),
    -- Find the point on mining boundary closest to that road point
    -- This ensures road only touches the boundary, not goes through
    boundary_exit AS (
      SELECT 
        -- Get boundary of mining site
        ST_Boundary(ms.mining_geom) as mining_boundary,
        -- Find point on boundary closest to road
        ST_ClosestPoint(ST_Boundary(ms.mining_geom), nr.road_point) as exit_pt,
        nr.road_point,
        nr.road_gid,
        ms.mining_geom
      FROM mining_site ms
      CROSS JOIN nearest_road nr
    )
    SELECT 
      $1::integer as mining_gid,
      ST_Transform(be.exit_pt, 4326) as entry_point,
      ST_Transform(be.road_point, 4326) as connection_point,
      be.road_gid,
      ST_AsGeoJSON(ST_Transform(ST_MakeLine(be.exit_pt, be.road_point), 4326))::jsonb as path_geom,
      ST_Distance(be.exit_pt, be.road_point) as path_length,
      ST_Distance(be.exit_pt
        be.road_point) as path_cost,
        true as connected_to_existing,
        -- Verify the path only touches boundary, doesn't cross interior
        ST_Intersects(ST_MakeLine(be.exit_pt, be.road_point), ST_Boundary(be.mining_geom)) as touches_boundary,
        NOT ST_Crosses(ST_MakeLine(be.exit_pt, be.road_point), be.mining_geom) as does_not_cross
      FROM boundary_exit be
    `, [miningGid]);
    
    if (result.rows.length === 0) return res.status(404).json({ error: 'Mining site not found' });
    
    const row = result.rows[0];
    
    // Safety check: ensure road doesn't pass through mining site
    if (!row.does_not_cross) {
      console.warn(`Warning: Road for mining site ${miningGid} crosses the site interior`);
    }
    
    res.json({
      miningGid: row.mining_gid,
      entryPoint: row.entry_point,
      connectionPoint: row.connection_point,
      roadGid: row.road_gid,
      pathLength: parseFloat(row.path_length),
      pathCost: parseFloat(row.path_cost),
      connectedToExisting: row.connected_to_existing,
      geometry: row.path_geom,
      valid: row.does_not_cross
    });
  }));
  
  // OPTIMIZED: Generate roads for ALL mining sites with strict boundary-only connection
  app.post('/api/generate-all-roads', asyncHandler(async (req, res) => {
    const { batchSize = null, schoolBuffer = 500 } = req.body;
    
    // Clear existing
    await pool.query('TRUNCATE mining_connection_status');
    await pool.query("DELETE FROM road_network WHERE road_type = 'mining_access'");
    
    // Get total count
    const countResult = await pool.query(`SELECT COUNT(*) as total FROM gorakhpur_brickkiln WHERE geom IS NOT NULL`);
    const totalSites = parseInt(countResult.rows[0].total);
    const limit = batchSize ? parseInt(batchSize) : totalSites;
    
    // Process in chunks for memory efficiency
    const CHUNK_SIZE = 50;
    let processed = 0;
    let totalLength = 0;
    const failedSites = [];
    
    for (let offset = 0; offset < limit; offset += CHUNK_SIZE) {
      const chunkLimit = Math.min(CHUNK_SIZE, limit - offset);
      
      // Get chunk of mining sites
      const miningSites = await pool.query(`
        SELECT gid, geom as mining_geom, ST_Centroid(geom) as center
        FROM gorakhpur_brickkiln
        WHERE geom IS NOT NULL
        ORDER BY gid
        LIMIT $1 OFFSET $2
      `, [chunkLimit, offset]);
      
      // Process each site
      for (const site of miningSites.rows) {
        try {
          // Find nearest road point to mining center
          const nearestRoad = await pool.query(`
            SELECT 
              rn.gid as road_gid,
              ST_ClosestPoint(rn.geom, $1) as road_point
            FROM road_network rn
            ORDER BY rn.geom <-> $1
            LIMIT 1
          `, [site.center]);
          
          if (nearestRoad.rows.length === 0) {
            failedSites.push(site.gid);
            continue;
          }
          
          const road = nearestRoad.rows[0];
          
          // CRITICAL: Get boundary point closest to road point
          // This ensures road starts at boundary and goes OUTWARD only
          const boundaryResult = await pool.query(`
            SELECT 
              ST_Boundary($1::geometry) as boundary_line,
              -- Point on boundary closest to road (this is our exit point)
              ST_ClosestPoint(ST_Boundary($1::geometry), $2::geometry) as exit_pt
          `, [site.mining_geom, road.road_point]);
          
          const exit_pt = boundaryResult.rows[0].exit_pt;
          
          // Create path from boundary to road
          const pathResult = await pool.query(`
            SELECT 
              ST_MakeLine($1::geometry, $2::geometry) as path_geom,
              ST_Distance($1::geometry, $2::geometry) as path_length,
              -- Validation: ensure path doesn't cross mining site interior
              NOT ST_Crosses(ST_MakeLine($1::geometry, $2::geometry), $3::geometry) as valid_path
          `, [exit_pt, road.road_point, site.mining_geom]);
          
          const { path_geom, path_length, valid_path } = pathResult.rows[0];
          
          if (!valid_path) {
            console.warn(`Invalid path for site ${site.gid}: crosses mining site`);
            // Try alternative: find point on boundary in opposite direction
            const altResult = await pool.query(`
              WITH boundary_points AS (
                SELECT (ST_DumpPoints(ST_Boundary($1::geometry))).geom as bp
              )
              SELECT bp as alt_exit_pt
              FROM boundary_points
              ORDER BY ST_Distance(bp, $2::geometry) 
              LIMIT 1
            `, [site.mining_geom, road.road_point]);
            
            if (altResult.rows.length === 0) {
              failedSites.push(site.gid);
              continue;
            }
            
            const alt_exit = altResult.rows[0].alt_exit_pt;
            const altPath = await pool.query(`
              SELECT 
                ST_MakeLine($1::geometry, $2::geometry) as path_geom,
                ST_Distance($1::geometry, $2::geometry) as path_length,
                NOT ST_Crosses(ST_MakeLine($1::geometry, $2::geometry), $3::geometry) as valid_path
            `, [alt_exit, road.road_point, site.mining_geom]);
            
            if (!altPath.rows[0].valid_path) {
              failedSites.push(site.gid);
              continue;
            }
            
            const altPathData = altPath.rows[0];
            const pathLengthNum = Math.round(parseFloat(altPathData.path_length) * 100) / 100;
            
            // Insert with alternative path
            await pool.query(`
              INSERT INTO mining_connection_status (
                mining_gid, is_connected, connection_road_gid, 
                connection_cost, connected_at, entry_point_geom, path_length
              ) VALUES ($1::integer, true, $2::integer, $3::numeric, NOW(), $4, $5::numeric)
            `, [site.gid, road.road_gid, pathLengthNum, alt_exit, pathLengthNum]);
            
            await pool.query(`
              INSERT INTO road_network (road_type, source_mining_site, length_km, cost, reverse_cost, geom)
              VALUES ('mining_access', $1::integer, $2::numeric / 1000, $2::numeric, $2::numeric, $3)
            `, [site.gid, pathLengthNum, altPathData.path_geom]);
            
            processed++;
            totalLength += pathLengthNum;
            continue;
          }
          
          const pathLengthNum = Math.round(parseFloat(path_length) * 100) / 100;
          
          // Insert connection status
          await pool.query(`
            INSERT INTO mining_connection_status (
              mining_gid, is_connected, connection_road_gid, 
              connection_cost, connected_at, entry_point_geom, path_length
            ) VALUES ($1::integer, true, $2::integer, $3::numeric, NOW(), $4, $5::numeric)
          `, [site.gid, road.road_gid, pathLengthNum, exit_pt, pathLengthNum]);
          
          // Insert new road into network (available for next sites)
          await pool.query(`
            INSERT INTO road_network (road_type, source_mining_site, length_km, cost, reverse_cost, geom)
            VALUES ('mining_access', $1::integer, $2::numeric / 1000, $2::numeric, $2::numeric, $3)
          `, [site.gid, pathLengthNum, path_geom]);
          
          processed++;
          totalLength += pathLengthNum;
          
        } catch (err) {
          console.error(`Error processing mining site ${site.gid}:`, err.message);
          failedSites.push(site.gid);
        }
      }
    }
    
    res.json({
      processedCount: processed,
      totalRoadLength: totalLength,
      failedSites: failedSites,
      totalSites: totalSites,
      success: failedSites.length === 0
    });
  }));
  
  // Reset network
  app.post('/api/reset-network', asyncHandler(async (req, res) => {
    await pool.query('TRUNCATE road_network');
    await pool.query('TRUNCATE mining_connection_status');
    
    // Re-insert highways
    await pool.query(`
      INSERT INTO road_network (road_type, length_km, cost, reverse_cost, geom)
      SELECT 
        'highway', length_km, length_km::double precision, length_km::double precision,
        ST_GeometryN(geom, 1)::geometry(LineString, 32644)
      FROM national_highway_2018 WHERE geom IS NOT NULL
    `);
    
    res.json({ success: true, message: 'Network reset' });
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
  
  // Future-proof: Add new road type endpoint
  app.post('/api/add-road-type', asyncHandler(async (req, res) => {
    const { tableName, roadTypeName } = req.body;
    
    // This endpoint allows adding new road tables dynamically
    // For now, just verify the table exists
    const check = await pool.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = $1
      ) as exists
    `, [tableName]);
    
    if (!check.rows[0].exists) {
      return res.status(404).json({ error: 'Table does not exist' });
    }
    
    // Insert roads from new table into network
    // This is a template - adjust column names based on actual table structure
    res.json({ 
      success: true, 
      message: `Road type ${roadTypeName} from ${tableName} can be added to network`,
      note: 'Please customize the insert query based on your table schema'
    });
  }));
  
  // Error handler
  app.use((err, req, res, next) => {
    console.error('Error:', err);
    res.status(500).json({ error: err.message, detail: err.detail || 'Internal error' });
  });
  
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Road router ready for highways + future road types`);
  });