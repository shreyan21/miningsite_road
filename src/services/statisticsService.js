import { MINING_SITE_FILTER, TABLES } from '../config/datasets.js';
import { qualifiedTable, quoteIdentifier, quoteLiteral } from '../utils/sql.js';

const buildMiningSiteWhereClause = (tableAlias = null) => {
  const columnPrefix = tableAlias ? `${tableAlias}.` : '';
  const values = MINING_SITE_FILTER.includedValues.map((value) => quoteLiteral(value)).join(', ');
  return `${columnPrefix}${quoteIdentifier(MINING_SITE_FILTER.nameColumn)} IN (${values})`;
};

export const getStatistics = async (pool) => {
  const result = await pool.query(`
    WITH totals AS (
      SELECT COUNT(*)::int AS total_mining_sites
      FROM ${qualifiedTable(TABLES.miningSites)}
      WHERE ${buildMiningSiteWhereClause()}
    ),
    status_counts AS (
      SELECT
        COUNT(*) FILTER (WHERE is_connected)::int AS connected_sites,
        COUNT(*) FILTER (WHERE NOT is_connected)::int AS blocked_sites,
        COUNT(*)::int AS processed_sites
      FROM mining_connection_status
    )
    SELECT
      totals.total_mining_sites,
      status_counts.connected_sites,
      status_counts.blocked_sites,
      status_counts.processed_sites,
      (totals.total_mining_sites - status_counts.processed_sites)::int AS pending_sites,
      (SELECT COUNT(*)::int FROM road_network WHERE source_mining_site IS NOT NULL) AS new_roads_count,
      (SELECT COALESCE(SUM(length_km), 0) FROM road_network WHERE source_mining_site IS NOT NULL) AS new_roads_length,
      (SELECT COALESCE(SUM(path_length), 0) FROM mining_connection_status WHERE is_connected) AS total_path_length_m
    FROM totals, status_counts
  `);

  return result.rows[0];
};
