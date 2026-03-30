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
  generateRoutesForSelectedMiningSites,
  generateRoutesForMiningSitesWithProgress,
  MAX_BATCH_SELECTION,
  removeRoutesForMiningSites,
  resetRoadNetwork,
} from './src/services/routePlannerService.js';
import { getStatistics } from './src/services/statisticsService.js';
import { registerRoadSource, syncRoadSources } from './src/services/roadSourceService.js';

const app = express();
const PORT = process.env.PORT || 5000;
const generationJobs = new Map();

app.use(cors());
app.use(express.json({ limit: '2mb' }));

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

const parsePositiveNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};

const createGenerationJob = ({ batchSize, schoolBuffer, appendMode }) => {
  const id = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const job = {
    id,
    status: 'queued',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    input: {
      batchSize,
      schoolBuffer,
      appendMode,
    },
    progress: {
      stage: 'queued',
      processedSites: 0,
      connectedSites: 0,
      failedSites: 0,
      percentComplete: 0,
      maximumBatchSize: MAX_BATCH_SELECTION,
    },
    result: null,
    error: null,
  };

  generationJobs.set(id, job);
  return job;
};

const updateGenerationJob = (jobId, patch) => {
  const job = generationJobs.get(jobId);
  if (!job) return;

  job.updatedAt = new Date().toISOString();
  if (patch.status) {
    job.status = patch.status;
  }
  if (patch.progress) {
    job.progress = {
      ...job.progress,
      ...patch.progress,
    };
  }
  if (patch.result) {
    job.result = patch.result;
  }
  if (patch.error) {
    job.error = patch.error;
  }
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

app.get('/api/runtime-config', asyncHandler(async (req, res) => {
  res.json({
    maximumBatchSize: MAX_BATCH_SELECTION,
    topBanner: {
      type: 'info',
      message: `A maximum of ${MAX_BATCH_SELECTION} sites can be selected in one run.`,
      placement: 'top',
    },
  });
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
  const runAsync = req.body.async === true;

  if (Number.isInteger(batchSize) && batchSize > MAX_BATCH_SELECTION) {
    return res.status(400).json({
      error: `A maximum of ${MAX_BATCH_SELECTION} sites can be processed in one request.`,
      maximumBatchSize: MAX_BATCH_SELECTION,
      requestedSites: batchSize,
      topBanner: {
        type: 'warning',
        message: `A maximum of ${MAX_BATCH_SELECTION} sites can be selected in one run.`,
        placement: 'top',
      },
    });
  }

  if (runAsync) {
    const job = createGenerationJob({
      batchSize: Number.isInteger(batchSize) && batchSize > 0 ? batchSize : null,
      schoolBuffer,
      appendMode,
    });

    updateGenerationJob(job.id, {
      status: 'running',
      progress: {
        stage: 'starting',
      },
    });

    generateRoutesForMiningSitesWithProgress(pool, {
      batchSize: Number.isInteger(batchSize) && batchSize > 0 ? batchSize : null,
      schoolBuffer,
      appendMode,
      onProgress: async (progress) => {
        updateGenerationJob(job.id, {
          status: progress.stage === 'completed' ? 'completed' : 'running',
          progress,
        });
      },
    })
      .then((summary) => {
        updateGenerationJob(job.id, {
          status: 'completed',
          result: summary,
          progress: {
            stage: 'completed',
            percentComplete: 100,
            processedSites: summary.selectedSites,
            connectedSites: summary.processedCount,
            failedSites: summary.failedCount,
            message: summary.message,
          },
        });
      })
      .catch((error) => {
        updateGenerationJob(job.id, {
          status: 'failed',
          error: {
            message: error.message || 'Generation failed.',
            detail: error.detail || null,
          },
        });
      });

    return res.status(202).json({
      success: true,
      async: true,
      jobId: job.id,
      maximumBatchSize: MAX_BATCH_SELECTION,
      topBanner: {
        type: 'info',
        message: `A maximum of ${MAX_BATCH_SELECTION} sites can be selected in one run.`,
        placement: 'top',
      },
      progress: job.progress,
    });
  }

  const summary = await generateRoutesForMiningSites(pool, {
    batchSize: Number.isInteger(batchSize) && batchSize > 0 ? batchSize : null,
    schoolBuffer,
    appendMode,
  });

  res.json({
    ...summary,
    topBanner: {
      type: 'info',
      message: `A maximum of ${MAX_BATCH_SELECTION} sites can be selected in one run.`,
      placement: 'top',
    },
  });
}));

app.get('/api/generate-all-roads-progress/:jobId', asyncHandler(async (req, res) => {
  const job = generationJobs.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: 'Generation job not found.' });
  }

  res.json(job);
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

app.post('/api/generate-selected-roads', asyncHandler(async (req, res) => {
  const miningGids = Array.isArray(req.body.miningGids) ? req.body.miningGids : [];
  const schoolBuffer = parsePositiveNumber(req.body.schoolBuffer, 500);

  const summary = await generateRoutesForSelectedMiningSites(pool, {
    miningGids,
    schoolBuffer,
    replaceExisting: req.body.replaceExisting !== false,
  });

  res.json(summary);
}));

app.post('/api/remove-selected-roads', asyncHandler(async (req, res) => {
  const miningGids = Array.isArray(req.body.miningGids) ? req.body.miningGids : [];
  const summary = await removeRoutesForMiningSites(pool, { miningGids });
  const stats = await getStatistics(pool);
  res.json({
    ...summary,
    statistics: stats,
  });
}));

app.post('/api/road-sources', asyncHandler(async (req, res) => {
  const payload = await registerRoadSource(pool, req.body);
  res.status(201).json(payload);
}));

app.post('/api/road-sources/sync', asyncHandler(async (req, res) => {
  const payload = await syncRoadSources(pool);
  res.json(payload);
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
