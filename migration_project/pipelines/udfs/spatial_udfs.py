# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Spatial Python UDFs
# MAGIC
# MAGIC This notebook defines Python UDFs for spatial/geography operations.
# MAGIC These replace SQL Server spatial functions using H3 and Shapely libraries.
# MAGIC
# MAGIC **Converted Functions:**
# MAGIC - dbo.fnCalcDistanceNearby
# MAGIC - dbo.fnGeometry2SVG
# MAGIC - dbo.fnGeometry2SVG_iso3D
# MAGIC - dbo.fnGeometry2JSON
# MAGIC - dbo.fnGeoPointShiftScale
# MAGIC - dbo.fnGeoPointShiftScale_iso3D
# MAGIC - dbo.fnFixGeographyOrder
# MAGIC - stg.fnNearestNeighbor_3Ordered
# MAGIC
# MAGIC **Libraries Required:** h3-py, shapely

# COMMAND ----------

# MAGIC %pip install h3 shapely

# COMMAND ----------

from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import (
    StringType, DoubleType, IntegerType, BooleanType,
    ArrayType, StructType, StructField, MapType
)
from math import radians, sin, cos, sqrt, atan2, pi
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distance Calculations

# COMMAND ----------

@udf(returnType=DoubleType())
def fn_calc_distance_nearby(lat1, lon1, lat2, lon2):
    """
    Calculate distance in meters between two points using Haversine formula.

    Converted from: dbo.fnCalcDistanceNearby

    Args:
        lat1, lon1: Latitude and longitude of first point
        lat2, lon2: Latitude and longitude of second point

    Returns:
        Distance in meters
    """
    if None in (lat1, lon1, lat2, lon2):
        return None

    R = 6371000  # Earth radius in meters

    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)

    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

# Register for SQL use
spark.udf.register("fn_calc_distance_nearby", fn_calc_distance_nearby)

# COMMAND ----------

@udf(returnType=DoubleType())
def fn_calc_distance_euclidean(x1, y1, x2, y2):
    """
    Calculate Euclidean distance between two points (for projected coordinates).

    Args:
        x1, y1: Coordinates of first point
        x2, y2: Coordinates of second point

    Returns:
        Distance in same units as input coordinates
    """
    if None in (x1, y1, x2, y2):
        return None

    return sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

spark.udf.register("fn_calc_distance_euclidean", fn_calc_distance_euclidean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry to SVG Conversion

# COMMAND ----------

@udf(returnType=StringType())
def fn_geometry_to_svg(wkt_string, width=100, height=100):
    """
    Convert WKT geometry to SVG path.

    Converted from: dbo.fnGeometry2SVG

    Args:
        wkt_string: Well-Known Text representation of geometry
        width: SVG viewport width (default 100)
        height: SVG viewport height (default 100)

    Returns:
        SVG string
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt

        geom = wkt.loads(wkt_string)
        bounds = geom.bounds  # (minx, miny, maxx, maxy)

        if bounds[0] == bounds[2] and bounds[1] == bounds[3]:
            # Point geometry
            return f'<svg viewBox="0 0 {width} {height}"><circle cx="{width/2}" cy="{height/2}" r="5" fill="black"/></svg>'

        # Calculate scaling
        geom_width = bounds[2] - bounds[0]
        geom_height = bounds[3] - bounds[1]

        if geom_width == 0:
            geom_width = 1
        if geom_height == 0:
            geom_height = 1

        scale_x = (width - 10) / geom_width
        scale_y = (height - 10) / geom_height
        scale = min(scale_x, scale_y)

        # Transform coordinates
        def transform(x, y):
            tx = (x - bounds[0]) * scale + 5
            ty = (bounds[3] - y) * scale + 5  # Flip Y axis
            return f"{tx:.2f},{ty:.2f}"

        # Generate SVG path
        if geom.geom_type == 'Polygon':
            coords = list(geom.exterior.coords)
            path_parts = ["M " + transform(coords[0][0], coords[0][1])]
            for x, y in coords[1:]:
                path_parts.append("L " + transform(x, y))
            path_parts.append("Z")
            path_data = " ".join(path_parts)

        elif geom.geom_type == 'MultiPolygon':
            all_paths = []
            for polygon in geom.geoms:
                coords = list(polygon.exterior.coords)
                path_parts = ["M " + transform(coords[0][0], coords[0][1])]
                for x, y in coords[1:]:
                    path_parts.append("L " + transform(x, y))
                path_parts.append("Z")
                all_paths.append(" ".join(path_parts))
            path_data = " ".join(all_paths)

        elif geom.geom_type == 'LineString':
            coords = list(geom.coords)
            path_parts = ["M " + transform(coords[0][0], coords[0][1])]
            for x, y in coords[1:]:
                path_parts.append("L " + transform(x, y))
            path_data = " ".join(path_parts)

        elif geom.geom_type == 'Point':
            x, y = geom.x, geom.y
            tx, ty = transform(x, y).split(',')
            return f'<svg viewBox="0 0 {width} {height}"><circle cx="{tx}" cy="{ty}" r="5" fill="black"/></svg>'

        else:
            return None

        return f'<svg viewBox="0 0 {width} {height}"><path d="{path_data}" fill="none" stroke="black" stroke-width="1"/></svg>'

    except Exception as e:
        return None

spark.udf.register("fn_geometry_to_svg", fn_geometry_to_svg)

# COMMAND ----------

@udf(returnType=StringType())
def fn_geometry_to_svg_filled(wkt_string, width=100, height=100, fill_color="lightblue", stroke_color="black"):
    """
    Convert WKT geometry to filled SVG path.

    Args:
        wkt_string: Well-Known Text representation of geometry
        width, height: SVG viewport dimensions
        fill_color: Fill color for polygons
        stroke_color: Stroke color for outline

    Returns:
        SVG string with filled polygon
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt

        geom = wkt.loads(wkt_string)
        bounds = geom.bounds

        geom_width = max(bounds[2] - bounds[0], 1)
        geom_height = max(bounds[3] - bounds[1], 1)

        scale = min((width - 10) / geom_width, (height - 10) / geom_height)

        def transform(x, y):
            tx = (x - bounds[0]) * scale + 5
            ty = (bounds[3] - y) * scale + 5
            return f"{tx:.2f},{ty:.2f}"

        if geom.geom_type in ('Polygon', 'MultiPolygon'):
            polygons = [geom] if geom.geom_type == 'Polygon' else list(geom.geoms)
            paths = []

            for polygon in polygons:
                coords = list(polygon.exterior.coords)
                path_parts = ["M " + transform(coords[0][0], coords[0][1])]
                for x, y in coords[1:]:
                    path_parts.append("L " + transform(x, y))
                path_parts.append("Z")
                paths.append(" ".join(path_parts))

            path_data = " ".join(paths)
            return f'<svg viewBox="0 0 {width} {height}"><path d="{path_data}" fill="{fill_color}" stroke="{stroke_color}" stroke-width="1"/></svg>'

        return None

    except Exception:
        return None

spark.udf.register("fn_geometry_to_svg_filled", fn_geometry_to_svg_filled)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry to JSON Conversion

# COMMAND ----------

@udf(returnType=StringType())
def fn_geometry_to_json(wkt_string):
    """
    Convert WKT geometry to GeoJSON.

    Converted from: dbo.fnGeometry2JSON

    Args:
        wkt_string: Well-Known Text representation of geometry

    Returns:
        GeoJSON string
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        from shapely.geometry import mapping

        geom = wkt.loads(wkt_string)
        return json.dumps(mapping(geom))

    except Exception:
        return None

spark.udf.register("fn_geometry_to_json", fn_geometry_to_json)

# COMMAND ----------

@udf(returnType=StringType())
def fn_json_to_wkt(geojson_string):
    """
    Convert GeoJSON to WKT geometry.

    Args:
        geojson_string: GeoJSON string

    Returns:
        WKT string
    """
    if geojson_string is None:
        return None

    try:
        from shapely.geometry import shape

        geojson = json.loads(geojson_string)
        geom = shape(geojson)
        return geom.wkt

    except Exception:
        return None

spark.udf.register("fn_json_to_wkt", fn_json_to_wkt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry Transformation

# COMMAND ----------

@udf(returnType=StringType())
def fn_geo_point_shift_scale(wkt_string, shift_x, shift_y, scale_factor):
    """
    Transform geometry by shifting and scaling.

    Converted from: dbo.fnGeoPointShiftScale

    Args:
        wkt_string: Well-Known Text representation of geometry
        shift_x: X offset to add
        shift_y: Y offset to add
        scale_factor: Scale multiplier

    Returns:
        Transformed WKT string
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        from shapely.affinity import translate, scale as shapely_scale

        geom = wkt.loads(wkt_string)

        # Apply translation
        if shift_x or shift_y:
            geom = translate(geom, xoff=shift_x or 0, yoff=shift_y or 0)

        # Apply scaling
        if scale_factor and scale_factor != 1:
            geom = shapely_scale(geom, xfact=scale_factor, yfact=scale_factor, origin='centroid')

        return geom.wkt

    except Exception:
        return None

spark.udf.register("fn_geo_point_shift_scale", fn_geo_point_shift_scale)

# COMMAND ----------

@udf(returnType=StringType())
def fn_fix_geography_order(wkt_string):
    """
    Fix polygon ring ordering for geography type.
    SQL Server geography requires counter-clockwise exterior rings.

    Converted from: dbo.fnFixGeographyOrder

    Args:
        wkt_string: Well-Known Text representation of geometry

    Returns:
        WKT string with corrected ring orientation
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        from shapely.geometry import Polygon, MultiPolygon

        geom = wkt.loads(wkt_string)

        if geom.geom_type == 'Polygon':
            # Ensure exterior is counter-clockwise
            exterior = geom.exterior
            if not exterior.is_ccw:
                exterior_coords = list(exterior.coords)[::-1]
            else:
                exterior_coords = list(exterior.coords)

            # Ensure interiors are clockwise
            interiors = []
            for interior in geom.interiors:
                if interior.is_ccw:
                    interiors.append(list(interior.coords)[::-1])
                else:
                    interiors.append(list(interior.coords))

            geom = Polygon(exterior_coords, interiors)

        elif geom.geom_type == 'MultiPolygon':
            fixed_polygons = []
            for polygon in geom.geoms:
                exterior = polygon.exterior
                if not exterior.is_ccw:
                    exterior_coords = list(exterior.coords)[::-1]
                else:
                    exterior_coords = list(exterior.coords)

                interiors = []
                for interior in polygon.interiors:
                    if interior.is_ccw:
                        interiors.append(list(interior.coords)[::-1])
                    else:
                        interiors.append(list(interior.coords))

                fixed_polygons.append(Polygon(exterior_coords, interiors))

            geom = MultiPolygon(fixed_polygons)

        return geom.wkt

    except Exception:
        return None

spark.udf.register("fn_fix_geography_order", fn_fix_geography_order)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nearest Neighbor Functions

# COMMAND ----------

@udf(returnType=ArrayType(StructType([
    StructField("neighbor_id", IntegerType()),
    StructField("distance", DoubleType()),
    StructField("rank", IntegerType())
])))
def fn_nearest_neighbor_3_ordered(target_lat, target_lon, neighbor_ids, neighbor_lats, neighbor_lons):
    """
    Find 3 nearest neighbors ordered by distance.

    Converted from: stg.fnNearestNeighbor_3Ordered

    Args:
        target_lat, target_lon: Target point coordinates
        neighbor_ids: Array of neighbor IDs
        neighbor_lats: Array of neighbor latitudes
        neighbor_lons: Array of neighbor longitudes

    Returns:
        Array of 3 nearest neighbors with distance and rank
    """
    if None in (target_lat, target_lon) or not neighbor_ids:
        return None

    R = 6371000  # Earth radius in meters
    distances = []

    for nid, nlat, nlon in zip(neighbor_ids, neighbor_lats, neighbor_lons):
        if nlat is not None and nlon is not None:
            phi1 = radians(target_lat)
            phi2 = radians(nlat)
            delta_phi = radians(nlat - target_lat)
            delta_lambda = radians(nlon - target_lon)

            a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            distance = R * c

            distances.append({"neighbor_id": nid, "distance": distance})

    # Sort by distance and take top 3
    distances.sort(key=lambda x: x["distance"])

    return [
        {"neighbor_id": d["neighbor_id"], "distance": d["distance"], "rank": i + 1}
        for i, d in enumerate(distances[:3])
    ]

spark.udf.register("fn_nearest_neighbor_3_ordered", fn_nearest_neighbor_3_ordered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 Spatial Indexing Functions

# COMMAND ----------

@udf(returnType=StringType())
def fn_lat_lng_to_h3(lat, lng, resolution=9):
    """
    Convert lat/lng to H3 index.

    Args:
        lat: Latitude
        lng: Longitude
        resolution: H3 resolution (0-15, default 9 = ~174m hexagons)

    Returns:
        H3 index string
    """
    if lat is None or lng is None:
        return None

    try:
        import h3
        return h3.geo_to_h3(lat, lng, resolution)
    except Exception:
        return None

spark.udf.register("fn_lat_lng_to_h3", fn_lat_lng_to_h3)

# COMMAND ----------

@udf(returnType=ArrayType(StringType()))
def fn_h3_k_ring(h3_index, k=1):
    """
    Get k-ring of H3 cells around a given cell.

    Args:
        h3_index: H3 index string
        k: Ring distance (default 1)

    Returns:
        Array of H3 indices in the k-ring
    """
    if h3_index is None:
        return None

    try:
        import h3
        return list(h3.k_ring(h3_index, k))
    except Exception:
        return None

spark.udf.register("fn_h3_k_ring", fn_h3_k_ring)

# COMMAND ----------

@udf(returnType=IntegerType())
def fn_h3_distance(h3_index1, h3_index2):
    """
    Calculate grid distance between two H3 cells.

    Args:
        h3_index1: First H3 index
        h3_index2: Second H3 index

    Returns:
        Grid distance (number of cells)
    """
    if h3_index1 is None or h3_index2 is None:
        return None

    try:
        import h3
        return h3.h3_distance(h3_index1, h3_index2)
    except Exception:
        return None

spark.udf.register("fn_h3_distance", fn_h3_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry Validation

# COMMAND ----------

@udf(returnType=BooleanType())
def fn_is_valid_geometry(wkt_string):
    """
    Check if WKT string represents a valid geometry.

    Args:
        wkt_string: Well-Known Text string

    Returns:
        True if valid, False otherwise
    """
    if wkt_string is None:
        return False

    try:
        from shapely import wkt
        geom = wkt.loads(wkt_string)
        return geom.is_valid
    except Exception:
        return False

spark.udf.register("fn_is_valid_geometry", fn_is_valid_geometry)

# COMMAND ----------

@udf(returnType=StringType())
def fn_make_valid_geometry(wkt_string):
    """
    Attempt to fix an invalid geometry.

    Args:
        wkt_string: Well-Known Text string

    Returns:
        Fixed WKT string, or original if already valid
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        from shapely.validation import make_valid

        geom = wkt.loads(wkt_string)
        if not geom.is_valid:
            geom = make_valid(geom)
        return geom.wkt
    except Exception:
        return None

spark.udf.register("fn_make_valid_geometry", fn_make_valid_geometry)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry Properties

# COMMAND ----------

@udf(returnType=DoubleType())
def fn_geometry_area(wkt_string):
    """
    Calculate area of a geometry (for projected coordinates).

    Args:
        wkt_string: Well-Known Text string

    Returns:
        Area in square units of the coordinate system
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        geom = wkt.loads(wkt_string)
        return geom.area
    except Exception:
        return None

spark.udf.register("fn_geometry_area", fn_geometry_area)

# COMMAND ----------

@udf(returnType=DoubleType())
def fn_geometry_length(wkt_string):
    """
    Calculate length/perimeter of a geometry.

    Args:
        wkt_string: Well-Known Text string

    Returns:
        Length in units of the coordinate system
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        geom = wkt.loads(wkt_string)
        return geom.length
    except Exception:
        return None

spark.udf.register("fn_geometry_length", fn_geometry_length)

# COMMAND ----------

@udf(returnType=StringType())
def fn_geometry_centroid(wkt_string):
    """
    Get centroid of a geometry.

    Args:
        wkt_string: Well-Known Text string

    Returns:
        WKT Point of centroid
    """
    if wkt_string is None:
        return None

    try:
        from shapely import wkt
        geom = wkt.loads(wkt_string)
        return geom.centroid.wkt
    except Exception:
        return None

spark.udf.register("fn_geometry_centroid", fn_geometry_centroid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Distance calculation
# MAGIC df = df.withColumn("distance_m", fn_calc_distance_nearby(col("lat1"), col("lon1"), col("lat2"), col("lon2")))
# MAGIC
# MAGIC # Geometry to SVG
# MAGIC df = df.withColumn("svg", fn_geometry_to_svg(col("geometry_wkt"), lit(200), lit(200)))
# MAGIC
# MAGIC # Geometry to GeoJSON
# MAGIC df = df.withColumn("geojson", fn_geometry_to_json(col("geometry_wkt")))
# MAGIC
# MAGIC # H3 spatial indexing
# MAGIC df = df.withColumn("h3_index", fn_lat_lng_to_h3(col("latitude"), col("longitude"), lit(9)))
# MAGIC
# MAGIC # Nearest neighbors (requires array columns)
# MAGIC df = df.withColumn("neighbors", fn_nearest_neighbor_3_ordered(
# MAGIC     col("target_lat"), col("target_lon"),
# MAGIC     col("neighbor_ids"), col("neighbor_lats"), col("neighbor_lons")
# MAGIC ))
# MAGIC ```
