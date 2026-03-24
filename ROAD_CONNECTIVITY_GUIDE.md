# Mining Road Connectivity Guide

## What the current app now does

This codebase is now organized as a stable baseline for your mining-road workflow:

- It auto-creates the support tables `road_network`, `mining_connection_status`, and `road_source_registry`.
- It seeds the road network dynamically from registered road layers, starting with `national_highway_2018`.
- It treats new generated mining access roads as part of the active road network for later sites in the same run.
- It blocks direct connectors that intersect:
  - river polygons
  - user-buffered school zones
  - other mining site polygons

Current planner mode is intentionally honest: it is a `direct_connector` engine, not yet a full least-cost bypass engine.

## Your full target problem

The actual target problem is:

1. Start from the boundary of a mining polygon, not the center.
2. Connect to the nearest usable existing road network.
3. Minimize total construction cost.
4. Never cross:
   - rivers directly
   - buffered school areas
   - other mining sites
5. When a river blocks the direct path, route along the river edge until a valid end or detour point is found.
6. Once a new road is built, it becomes an eligible road for future mining sites.

## Recommended production architecture

### Database

Keep PostGIS as the source of truth.

Core tables:

- `gorakhpur_brickkiln`
- `uprsac_09xxxx_riverxxxxx_09042018`
- `uprsac_09xxxx_educschool_20132016`
- `gorakhpur_ps`
- `road_source_registry`
- `road_network`
- `mining_connection_status`

Recommended extra tables for the advanced solver:

- `cost_grid_cells`
- `cost_grid_edges`
- `routing_barriers`
- `routing_portals`
- `route_runs`

### Backend

Recommended Express modules:

- `src/services/bootstrap.js`
  Creates and seeds runtime tables.
- `src/services/mapService.js`
  Loads all GeoJSON layers for the UI.
- `src/services/roadSourceService.js`
  Registers future road tables dynamically.
- `src/services/routePlannerService.js`
  Holds the routing engine.
- `src/services/statisticsService.js`
  Dashboard totals and summaries.

### Frontend

React UI should stay simple and operational:

- configuration panel
- runtime school buffer input
- batch generation
- network reset
- statistics and blocked-site diagnostics
- map layers for roads, mines, rivers, schools, and school buffers

## How to solve the full least-cost routing problem

The correct long-term approach is not a single straight line query. Use a cost-surface or graph search.

### Option A: Grid-based least-cost path

Best when obstacles are polygon-heavy and you need strong control over costs.

Flow:

1. Build a study area around the target mining site and nearby roads.
2. Generate a grid of cells, for example 20 m to 50 m depending on accuracy and performance.
3. Mark cells as forbidden when they intersect:
   - school buffers
   - rivers
   - other mining sites
4. Assign movement costs to valid cells.
5. Lower the cost near existing roads so reuse is preferred.
6. Lower the cost for cells adjacent to already-created mining access roads.
7. Run shortest path from mining boundary cells to cells touching the active road network.
8. Convert the winning cell path to a smoothed linestring.

### Option B: Graph or visibility routing

Best when you want cleaner vector geometry and more exact bypass behavior.

Flow:

1. Build nodes from:
   - mining boundary candidate exit points
   - road network snap points
   - obstacle corner points
   - river-end portal points
2. Create edges only where visibility is valid.
3. Penalize sharp turns, long detours, and proximity to barriers.
4. Run Dijkstra or A* on the graph.

## River bypass requirement

For your river rule, the advanced engine should:

1. Detect the blocking river polygon.
2. Derive river-end portal candidates.
3. Allow travel parallel to the river boundary.
4. Rejoin the shortest valid path only after the river obstacle ends.

Practical implementation choices:

- If you have river centerlines, use them plus bank offset logic.
- If you only have river polygons, derive portal candidates from the river polygon skeleton or from major-axis end points.
- Store bypass segments with `is_bypass = true` in `road_network`.

## Cost model

A simple cost model can be:

- base cost = distance
- near existing road = lower multiplier
- near previously built mining access road = lower multiplier
- river crossing = forbidden
- school buffer = forbidden
- other mining polygon = forbidden
- steep turn or extreme curvature = penalty

## Important performance rules

- Keep all geometry in `EPSG:32644` for routing math.
- Transform to `4326` only for map output.
- Use `GIST` indexes on every geometry column.
- Restrict routing to a local study window per mining site.
- Batch process sites instead of trying to solve everything in one SQL monster query.

## Recommended next implementation step

If you want the app to fully satisfy the river-end bypass requirement, the next milestone should be:

1. build a local cost grid around each mining site
2. mark forbidden cells from schools, rivers, and other mines
3. bias cells near roads to reduce cost
4. run shortest path to the active road network
5. save the resulting connector into `road_network`

That is the cleanest path from your current data to a reliable production solver.
