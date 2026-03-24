export const getStatistics = async (pool) => {
  const result = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM gorakhpur_brickkiln) AS total_mining_sites,
      (SELECT COUNT(*)::int FROM mining_connection_status WHERE is_connected) AS connected_sites,
      (SELECT COUNT(*)::int FROM mining_connection_status WHERE NOT is_connected) AS blocked_sites,
      (SELECT COUNT(*)::int FROM road_network WHERE source_mining_site IS NOT NULL) AS new_roads_count,
      (SELECT COALESCE(SUM(length_km), 0) FROM road_network WHERE source_mining_site IS NOT NULL) AS new_roads_length,
      (SELECT COALESCE(SUM(path_length), 0) FROM mining_connection_status WHERE is_connected) AS total_path_length_m
  `);

  return result.rows[0];
};
