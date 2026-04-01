export const DEFAULT_SRID = 32644;

export const TABLES = {
  miningSites: 'gorakhpur_brickkiln',
  forbiddenMining: 'forbidden_mining',
  rivers: 'uprsac_09xxxx_riverxxxxx_09042018',
};

export const MINING_SITE_FILTER = {
  nameColumn: 'name',
  includedValues: ['Mining Field'],
};

export const SCHOOL_SOURCES = [
  {
    tableName: 'uprsac_09xxxx_educschool_20132016',
    idColumn: 'gid',
    geomColumn: 'geom',
    nameColumn: 'schname',
    districtColumn: 'districtna',
    label: 'Education School',
    useForDisplay: true,
    useForObstacles: true,
  },
  {
    tableName: 'gorakhpur_ps',
    idColumn: 'gid',
    geomColumn: 'geom',
    nameColumn: 'field1',
    districtColumn: 'field2',
    label: 'Point School',
    useForDisplay: true,
    useForObstacles: true,
  },
  {
    tableName: 'school_buffer',
    idColumn: 'id',
    geomColumn: 'geom',
    isBuffered: true,
    label: 'Imported School Buffer',
    useForDisplay: false,
    useForObstacles: true,
  },
];

export const DEFAULT_ROAD_SOURCES = [
  {
    tableName: 'national_highway_2018',
    roadType: 'highway',
    geomColumn: 'geom',
    lengthColumn: 'length_km',
    roadCodeColumn: 'tr_rdcode',
  },
];

export const PLANNER_METADATA = {
  version: 'v2-obstacle-aware-polyline',
  description:
    'Creates obstacle-aware polylines from a mining boundary to the active road network, adding detours around school buffers, rivers, and other mining polygons when possible.',
  nextStep:
    'For fully optimal least-cost routing, the next upgrade is still a grid or graph cost-surface solver.',
};
