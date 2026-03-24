import { reseedBaseRoadNetwork } from './bootstrap.js';
import { columnExists, tableExists } from '../utils/sql.js';

export const registerRoadSource = async (pool, payload) => {
  const tableName = String(payload.tableName || '').trim();
  const roadType = String(payload.roadType || '').trim();
  const geomColumn = String(payload.geomColumn || 'geom').trim();
  const lengthColumn = payload.lengthColumn ? String(payload.lengthColumn).trim() : null;
  const roadCodeColumn = payload.roadCodeColumn ? String(payload.roadCodeColumn).trim() : null;

  if (!tableName || !roadType) {
    throw new Error('tableName and roadType are required.');
  }

  if (!(await tableExists(pool, tableName))) {
    throw new Error(`Table ${tableName} does not exist.`);
  }

  if (!(await columnExists(pool, tableName, geomColumn))) {
    throw new Error(`Column ${geomColumn} does not exist on ${tableName}.`);
  }

  if (lengthColumn && !(await columnExists(pool, tableName, lengthColumn))) {
    throw new Error(`Column ${lengthColumn} does not exist on ${tableName}.`);
  }

  if (roadCodeColumn && !(await columnExists(pool, tableName, roadCodeColumn))) {
    throw new Error(`Column ${roadCodeColumn} does not exist on ${tableName}.`);
  }

  await pool.query(
    `INSERT INTO road_source_registry (table_name, road_type, geom_column, length_column, road_code_column)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (table_name)
     DO UPDATE SET
       road_type = EXCLUDED.road_type,
       geom_column = EXCLUDED.geom_column,
       length_column = EXCLUDED.length_column,
       road_code_column = EXCLUDED.road_code_column,
       enabled = true`,
    [tableName, roadType, geomColumn, lengthColumn, roadCodeColumn],
  );

  await reseedBaseRoadNetwork(pool);

  return {
    success: true,
    message: `Registered ${tableName} as road source ${roadType}.`,
  };
};
