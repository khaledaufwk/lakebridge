# Databricks notebook source
"""
H3 Spatial Indexing UDFs

Provides UDFs for H3 hexagonal spatial indexing:
- point_to_h3: Convert lat/lon to H3 cell index
- polygon_to_h3_cells: Convert WKT polygon to H3 cell coverage
"""

import h3
from shapely import wkt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

# Default resolution: 9 (~175m hexagons, suitable for construction zones)
DEFAULT_H3_RESOLUTION = 9

@F.udf(returnType=StringType())
def point_to_h3(lat, lon, resolution=DEFAULT_H3_RESOLUTION):
    """Convert latitude/longitude to H3 cell index."""
    if lat is None or lon is None:
        return None
    try:
        return h3.geo_to_h3(float(lat), float(lon), resolution)
    except Exception:
        return None

@F.udf(returnType=ArrayType(StringType()))
def polygon_to_h3_cells(wkt_string, resolution=DEFAULT_H3_RESOLUTION):
    """Convert WKT polygon to list of H3 cells that cover it."""
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        if geom.is_empty or not geom.is_valid:
            return None
        # polyfill returns set of H3 indexes covering the polygon
        cells = h3.polyfill_geojson(geom.__geo_interface__, resolution)
        return list(cells)
    except Exception:
        return None

def extract_lat_lon_from_point_wkt(point_wkt_col):
    """
    Extract latitude and longitude from a WKT POINT string.
    Input: "POINT(longitude latitude)" or "POINT (longitude latitude)"
    Output: Two columns (latitude, longitude)
    """
    # Remove "POINT(" or "POINT (" prefix and ")" suffix
    coords = F.regexp_replace(point_wkt_col, r"POINT\s*\(", "")
    coords = F.regexp_replace(coords, r"\)", "")
    coords = F.trim(coords)

    # Split by space - WKT format is "lon lat"
    lon = F.split(coords, r"\s+").getItem(0).cast("double")
    lat = F.split(coords, r"\s+").getItem(1).cast("double")

    return lat, lon
