export const DEFAULT_SRID = 32644;

export const TABLES = {
  miningSites: 'gorakhpur_brickkiln',
  rivers: 'uprsac_09xxxx_riverxxxxx_09042018',
};

export const SCHOOL_SOURCES = [
  {
    tableName: 'uprsac_09xxxx_educschool_20132016',
    geomColumn: 'geom',
    nameColumn: 'schname',
    districtColumn: 'districtna',
    label: 'Education School',
  },
  {
    tableName: 'gorakhpur_ps',
    geomColumn: 'geom',
    nameColumn: 'field1',
    districtColumn: 'field2',
    label: 'Point School',
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
