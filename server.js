import express from 'express';
import cors from 'cors';
import pool from './db.js';
import { DEFAULT_SRID } from './src/config/datasets.js';
import { bootstrapDatabase } from './src/services/bootstrap.js';
import {
  getMapLayers,
  getMiningSitesGeoJson,
  getObstacleGeoJson,
  getRoadNetworkGeoJson,
  getRoadSourceSummary,
  getSchoolsGeoJson,
} from './src/services/mapService.js';
import {
  calculateRouteForMiningSite,
  generateRoutesForMiningSites,
  resetRoadNetwork,
} from './src/services/routePlannerService.js';
import { getStatistics } from './src/services/statisticsService.js';
import { registerRoadSource } from './src/services/roadSourceService.js';

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json({ limit: '2mb' }));

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

const parsePositiveNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};

app.get('/api/health', asyncHandler(async (req, res) => {
  const result = await pool.query('SELECT NOW() AS now, current_database() AS db');
  res.json({
    status: 'ok',
    database: result.rows[0].db,
    srid: DEFAULT_SRID,
    timestamp: result.rows[0].now,
  });
}));

app.get('/api/map-layers', asyncHandler(async (req, res) => {
  const schoolBuffer = parsePositiveNumber(req.query.schoolBuffer, 500);
  const payload = await getMapLayers(pool, {
    schoolBuffer,
    includeSchools: req.query.includeSchools !== 'false',
    includeObstacles: req.query.includeObstacles === 'true',
    includeRoadSources: req.query.includeRoadSources === 'true',
  });
  res.json(payload);
}));

app.get('/api/mining-sites', asyncHandler(async (req, res) => {
  const geojson = await getMiningSitesGeoJson(pool);
  res.json(geojson);
}));

app.get('/api/roads', asyncHandler(async (req, res) => {
  const geojson = await getRoadNetworkGeoJson(pool);
  res.json(geojson);
}));

app.get('/api/schools', asyncHandler(async (req, res) => {
  const geojson = await getSchoolsGeoJson(pool);
  res.json(geojson);
}));

app.get('/api/obstacles', asyncHandler(async (req, res) => {
  const schoolBuffer = parsePositiveNumber(req.query.schoolBuffer, 500);
  const requestedTypes = String(req.query.types || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const allowAll = requestedTypes.length === 0;

  const geojson = await getObstacleGeoJson(pool, {
    schoolBuffer,
    includeSchoolBuffers: allowAll || requestedTypes.includes('school_buffer'),
    includeRivers: allowAll || requestedTypes.includes('river'),
    includeMiningSites: allowAll || requestedTypes.includes('mining_site'),
  });
  res.json(geojson);
}));

app.get('/api/statistics', asyncHandler(async (req, res) => {
  const stats = await getStatistics(pool);
  res.json(stats);
}));

app.get('/api/road-sources', asyncHandler(async (req, res) => {
  const roadSources = await getRoadSourceSummary(pool);
  res.json({ roadSources });
}));

app.post('/api/calculate-route', asyncHandler(async (req, res) => {
  const miningGid = Number(req.body.miningGid);
  const schoolBuffer = parsePositiveNumber(req.body.schoolBuffer, 500);

  if (!Number.isInteger(miningGid) || miningGid <= 0) {
    return res.status(400).json({ error: 'A valid miningGid is required.' });
  }

  const result = await calculateRouteForMiningSite(pool, {
    miningGid,
    schoolBuffer,
  });

  if (!result) {
    return res.status(404).json({ error: `Mining site ${miningGid} was not found.` });
  }

  res.json(result);
}));

app.post('/api/generate-all-roads', asyncHandler(async (req, res) => {
  const batchSize = req.body.batchSize == null || req.body.batchSize === ''
    ? null
    : Number(req.body.batchSize);
  const schoolBuffer = parsePositiveNumber(req.body.schoolBuffer, 500);
  const appendMode = req.body.appendMode !== false;

  const summary = await generateRoutesForMiningSites(pool, {
    batchSize: Number.isInteger(batchSize) && batchSize > 0 ? batchSize : null,
    schoolBuffer,
    appendMode,
  });

  res.json(summary);
}));

app.post('/api/reset-network', asyncHandler(async (req, res) => {
  await resetRoadNetwork(pool);
  const stats = await getStatistics(pool);
  res.json({
    success: true,
    message: 'Road network reset to registered source roads.',
    statistics: stats,
  });
}));

app.post('/api/road-sources', asyncHandler(async (req, res) => {
  const payload = await registerRoadSource(pool, req.body);
  res.status(201).json(payload);
}));

app.use((err, req, res, next) => {
  console.error('Unhandled API error:', err);
  res.status(500).json({
    error: err.message || 'Internal server error',
    detail: err.detail || null,
  });
});

const startServer = async () => {
  await bootstrapDatabase(pool);
  app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
  });
};

startServer().catch(async (error) => {
  console.error('Startup failed:', error);
  try {
    await pool.end();
  } catch {
    // ignore shutdown errors during bootstrap failure
  }
  process.exit(1);
});
