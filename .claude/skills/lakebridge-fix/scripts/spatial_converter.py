"""
Spatial Function Converter for Lakebridge.

Converts T-SQL geography/geometry functions to Spark equivalents:
- geography::Point() → (lat, lon) columns + optional H3 index
- STDistance() → Haversine UDF
- STContains() → H3 containment or Shapely
- STIntersects() → H3 intersection
- STBuffer() → H3 ring
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import re


@dataclass
class SpatialFunctionInfo:
    """Information about a detected spatial function."""
    function_name: str           # e.g., "geography::Point", "STDistance"
    function_type: str           # "GEOGRAPHY", "GEOMETRY"
    line_number: int
    matched_text: str
    context: str                 # Surrounding code
    arguments: List[str]         # Parsed arguments


class SpatialConverter:
    """
    Convert T-SQL spatial functions to Spark equivalents.

    Provides templates for:
    - geography::Point() → (lat, lon) columns + H3 index
    - STDistance() → Haversine UDF
    - STContains() → H3 containment check
    - STIntersects() → H3 ring intersection
    - STBuffer() → H3 k-ring
    - STAsText() → WKT string (already text in Spark)
    """

    # Spatial function patterns
    SPATIAL_PATTERNS = {
        "GEOGRAPHY_POINT": {
            "pattern": r"geography::Point\s*\(\s*([^,]+),\s*([^,]+),\s*(\d+)\s*\)",
            "description": "Create point from lat/lon",
            "args": ["latitude", "longitude", "srid"],
        },
        "GEOGRAPHY_STGEOMFROMTEXT": {
            "pattern": r"geography::STGeomFromText\s*\(\s*([^,]+),\s*(\d+)\s*\)",
            "description": "Create geography from WKT",
            "args": ["wkt", "srid"],
        },
        "GEOMETRY_POINT": {
            "pattern": r"geometry::Point\s*\(\s*([^,]+),\s*([^,]+),\s*(\d+)\s*\)",
            "description": "Create geometry point",
            "args": ["x", "y", "srid"],
        },
        "ST_DISTANCE": {
            "pattern": r"\.STDistance\s*\(\s*([^)]+)\s*\)",
            "description": "Calculate distance between points",
            "args": ["other_point"],
        },
        "ST_CONTAINS": {
            "pattern": r"\.STContains\s*\(\s*([^)]+)\s*\)",
            "description": "Check if geometry contains point",
            "args": ["point"],
        },
        "ST_INTERSECTS": {
            "pattern": r"\.STIntersects\s*\(\s*([^)]+)\s*\)",
            "description": "Check if geometries intersect",
            "args": ["other_geometry"],
        },
        "ST_BUFFER": {
            "pattern": r"\.STBuffer\s*\(\s*([^)]+)\s*\)",
            "description": "Create buffer around geometry",
            "args": ["distance"],
        },
        "ST_ASTEXT": {
            "pattern": r"\.STAsText\s*\(\s*\)",
            "description": "Convert to WKT text",
            "args": [],
        },
        "ST_AREA": {
            "pattern": r"\.STArea\s*\(\s*\)",
            "description": "Calculate area",
            "args": [],
        },
        "ST_LENGTH": {
            "pattern": r"\.STLength\s*\(\s*\)",
            "description": "Calculate length",
            "args": [],
        },
        "ST_CENTROID": {
            "pattern": r"\.STCentroid\s*\(\s*\)",
            "description": "Get centroid point",
            "args": [],
        },
    }

    def detect_spatial_functions(self, sql: str) -> List[SpatialFunctionInfo]:
        """
        Detect spatial function usages in SQL.

        Returns:
            List of SpatialFunctionInfo objects
        """
        results = []
        lines = sql.split('\n')

        for func_name, func_info in self.SPATIAL_PATTERNS.items():
            pattern = func_info["pattern"]

            for match in re.finditer(pattern, sql, re.IGNORECASE):
                line_num = sql[:match.start()].count('\n') + 1

                # Get context (3 lines before and after)
                start_line = max(0, line_num - 4)
                end_line = min(len(lines), line_num + 3)
                context = '\n'.join(lines[start_line:end_line])

                # Extract arguments
                args = list(match.groups()) if match.groups() else []

                # Determine function type
                func_type = "GEOGRAPHY" if "GEOGRAPHY" in func_name or "geography" in match.group(0).lower() else "GEOMETRY"

                results.append(SpatialFunctionInfo(
                    function_name=func_name,
                    function_type=func_type,
                    line_number=line_num,
                    matched_text=match.group(0),
                    context=context,
                    arguments=args,
                ))

        return results

    def convert(self, spatial_functions: List[SpatialFunctionInfo], context: Dict) -> Tuple[str, List[str]]:
        """
        Generate Spark code for spatial function conversions.

        Args:
            spatial_functions: List of detected spatial functions
            context: Dict with configuration options

        Returns:
            Tuple of (converted_code, warnings)
        """
        warnings = []

        if not spatial_functions:
            return "", []

        # Determine which converters are needed
        needs_haversine = any(f.function_name == "ST_DISTANCE" for f in spatial_functions)
        needs_point_parse = any("POINT" in f.function_name for f in spatial_functions)
        needs_contains = any(f.function_name == "ST_CONTAINS" for f in spatial_functions)
        needs_h3 = context.get('use_h3', True) and (needs_contains or needs_point_parse)

        code_sections = []

        # Header
        code_sections.append('''# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Functions
# MAGIC
# MAGIC T-SQL geography/geometry functions converted to Spark equivalents.
# MAGIC
# MAGIC **Conversion approach:**
# MAGIC - geography::Point → (latitude, longitude) columns
# MAGIC - STDistance → Haversine formula UDF
# MAGIC - STContains → H3 hexagon containment (optional) or Shapely
# MAGIC - WKT parsing → Spark SQL string functions

# COMMAND ----------
''')

        # H3 setup if needed
        if needs_h3:
            code_sections.append(self._generate_h3_setup())
            warnings.append("H3 library required - ensure h3-pyspark is installed on cluster")

        # Point parsing
        if needs_point_parse:
            code_sections.append(self._generate_point_parser())

        # Haversine distance UDF
        if needs_haversine:
            code_sections.append(self._generate_haversine_udf())

        # Contains check
        if needs_contains:
            if context.get('use_h3', True):
                code_sections.append(self._generate_h3_contains())
            else:
                code_sections.append(self._generate_shapely_contains())

        # Add usage examples based on detected functions
        code_sections.append(self._generate_usage_examples(spatial_functions))

        return '\n'.join(code_sections), warnings

    def _generate_h3_setup(self) -> str:
        """Generate H3 library setup code."""
        return '''
# H3 Spatial Library Setup
# Install H3 if needed: %pip install h3 h3-pyspark

try:
    import h3
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, ArrayType, BooleanType

    # UDF to convert lat/lon to H3 index
    @udf(returnType=StringType())
    def geo_to_h3(lat, lon, resolution=9):
        """Convert lat/lon to H3 hexagon index."""
        if lat is None or lon is None:
            return None
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except Exception:
            return None

    # UDF to get H3 ring (neighboring hexagons)
    @udf(returnType=ArrayType(StringType()))
    def h3_k_ring(h3_index, k=1):
        """Get k-ring of hexagons around index."""
        if h3_index is None:
            return None
        try:
            return list(h3.k_ring(h3_index, k))
        except Exception:
            return None

    # Register UDFs for SQL usage
    spark.udf.register("geo_to_h3", geo_to_h3)
    spark.udf.register("h3_k_ring", h3_k_ring)

    print("H3 spatial library loaded successfully")
    H3_AVAILABLE = True

except ImportError:
    print("WARNING: H3 library not available. Install with: %pip install h3 h3-pyspark")
    H3_AVAILABLE = False
'''

    def _generate_point_parser(self) -> str:
        """Generate WKT point parsing code."""
        return '''
# COMMAND ----------

# WKT Point Parsing
# Converts "POINT (longitude latitude)" to separate lat/lon columns
# Note: WKT uses (longitude latitude) order, not (latitude longitude)

from pyspark.sql import functions as F

def parse_wkt_point_columns(df, wkt_column, lat_col="Latitude", lon_col="Longitude"):
    """
    Parse WKT POINT column into separate latitude and longitude columns.

    Args:
        df: Source DataFrame
        wkt_column: Name of column containing WKT POINT string
        lat_col: Output latitude column name
        lon_col: Output longitude column name

    Returns:
        DataFrame with added lat/lon columns
    """
    return (
        df
        # Remove "POINT (" prefix and ")" suffix
        .withColumn(
            "_wkt_clean",
            F.regexp_replace(F.col(wkt_column), r"POINT\\s*\\(", "")
        )
        .withColumn(
            "_wkt_clean",
            F.regexp_replace(F.col("_wkt_clean"), r"\\)", "")
        )
        # Split into longitude and latitude (WKT order)
        .withColumn(
            lon_col,
            F.split(F.trim(F.col("_wkt_clean")), "\\s+").getItem(0).cast("double")
        )
        .withColumn(
            lat_col,
            F.split(F.trim(F.col("_wkt_clean")), "\\s+").getItem(1).cast("double")
        )
        .drop("_wkt_clean")
    )


# Alternative: Direct parsing without intermediate column
# df = df.withColumn("Longitude", F.split(F.regexp_replace("PointWKT", r"POINT\\s*\\(|\\)", ""), "\\s+").getItem(0).cast("double"))
# df = df.withColumn("Latitude", F.split(F.regexp_replace("PointWKT", r"POINT\\s*\\(|\\)", ""), "\\s+").getItem(1).cast("double"))

print("WKT point parser loaded")
'''

    def _generate_haversine_udf(self) -> str:
        """Generate Haversine distance UDF."""
        return '''
# COMMAND ----------

# Haversine Distance UDF
# Equivalent to T-SQL geography.STDistance()
# Calculates great-circle distance in meters between two points

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import math

@udf(returnType=DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate great-circle distance between two points in meters.

    Uses the Haversine formula for accuracy on Earth's surface.
    Equivalent to T-SQL: point1.STDistance(point2)

    Args:
        lat1, lon1: First point coordinates (degrees)
        lat2, lon2: Second point coordinates (degrees)

    Returns:
        Distance in meters
    """
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None

    # Earth radius in meters
    R = 6371000

    # Convert to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_phi / 2) ** 2 + \\
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


# Optimized version using Spark SQL functions (faster for large datasets)
def haversine_distance_expr(lat1_col, lon1_col, lat2_col, lon2_col):
    """
    Create Spark SQL expression for Haversine distance.

    More efficient than UDF for large datasets.

    Usage:
        df.withColumn("distance_m", haversine_distance_expr("lat1", "lon1", "lat2", "lon2"))
    """
    from pyspark.sql import functions as F

    R = 6371000  # Earth radius in meters

    lat1 = F.radians(F.col(lat1_col))
    lat2 = F.radians(F.col(lat2_col))
    delta_lat = F.radians(F.col(lat2_col) - F.col(lat1_col))
    delta_lon = F.radians(F.col(lon2_col) - F.col(lon1_col))

    a = (
        F.pow(F.sin(delta_lat / 2), 2) +
        F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(delta_lon / 2), 2)
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    return F.lit(R) * c


# Register UDF for SQL usage
spark.udf.register("haversine_distance", haversine_distance)

print("Haversine distance UDF loaded")
'''

    def _generate_h3_contains(self) -> str:
        """Generate H3-based containment check."""
        return '''
# COMMAND ----------

# H3-Based Containment Check
# Equivalent to T-SQL polygon.STContains(point)
# Uses H3 hexagons for fast spatial containment queries

from pyspark.sql.functions import udf, col, array_contains
from pyspark.sql.types import BooleanType

if H3_AVAILABLE:
    @udf(returnType=BooleanType())
    def h3_point_in_polygon_hexes(point_h3, polygon_hexes):
        """
        Check if point's H3 index is in the polygon's hexagon set.

        This is an approximation - accuracy depends on H3 resolution.
        Resolution 9 (~174m) is good for most zone containment checks.

        Args:
            point_h3: H3 index of the point
            polygon_hexes: Array of H3 indices representing the polygon

        Returns:
            True if point is inside polygon
        """
        if point_h3 is None or polygon_hexes is None:
            return None
        return point_h3 in polygon_hexes

    spark.udf.register("h3_point_in_polygon", h3_point_in_polygon_hexes)


def create_polygon_h3_indices(polygon_wkt, resolution=9):
    """
    Convert polygon WKT to set of H3 indices.

    Use this to pre-compute H3 indices for zones/polygons,
    then join with point H3 indices for fast containment.

    Args:
        polygon_wkt: WKT representation of polygon
        resolution: H3 resolution (default 9 for ~174m hexagons)

    Returns:
        Set of H3 indices covering the polygon
    """
    try:
        import h3
        from shapely import wkt
        from shapely.geometry import mapping

        polygon = wkt.loads(polygon_wkt)
        geojson = mapping(polygon)

        # Get H3 indices that cover the polygon
        indices = h3.polyfill(geojson, resolution, geo_json_conformant=True)
        return set(indices)
    except Exception as e:
        print(f"Error creating H3 indices: {e}")
        return set()


# Example: Pre-compute zone H3 indices
# zone_hexes = create_polygon_h3_indices(zone_wkt, resolution=9)
# zone_hexes_broadcast = spark.sparkContext.broadcast(zone_hexes)
#
# @udf(returnType=BooleanType())
# def point_in_zone(point_h3):
#     return point_h3 in zone_hexes_broadcast.value
#
# df = df.withColumn("in_zone", point_in_zone(col("h3_index")))

print("H3 containment check loaded")
'''

    def _generate_shapely_contains(self) -> str:
        """Generate Shapely-based containment check."""
        return '''
# COMMAND ----------

# Shapely-Based Containment Check
# Equivalent to T-SQL polygon.STContains(point)
# Uses Shapely for precise geometric operations

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

try:
    from shapely import wkt as shapely_wkt
    from shapely.geometry import Point

    @udf(returnType=BooleanType())
    def point_in_polygon(lat, lon, polygon_wkt):
        """
        Check if point is inside polygon using Shapely.

        This is precise but slower than H3 for large datasets.

        Args:
            lat, lon: Point coordinates
            polygon_wkt: WKT representation of polygon

        Returns:
            True if point is inside polygon
        """
        if any(v is None for v in [lat, lon, polygon_wkt]):
            return None
        try:
            point = Point(lon, lat)  # Shapely uses (x, y) = (lon, lat)
            polygon = shapely_wkt.loads(polygon_wkt)
            return polygon.contains(point)
        except Exception:
            return None

    spark.udf.register("point_in_polygon", point_in_polygon)
    print("Shapely containment check loaded")
    SHAPELY_AVAILABLE = True

except ImportError:
    print("WARNING: Shapely not available. Install with: %pip install shapely")
    SHAPELY_AVAILABLE = False


# For better performance with many polygons, consider:
# 1. Pre-filter using bounding boxes
# 2. Use spatial indexing (R-tree)
# 3. Convert to H3 for approximate but faster checks
'''

    def _generate_usage_examples(self, spatial_functions: List[SpatialFunctionInfo]) -> str:
        """Generate usage examples based on detected functions."""
        examples = ['''
# COMMAND ----------

# MAGIC %md
# MAGIC ### Spatial Function Usage Examples
# MAGIC
# MAGIC Based on detected patterns in the source procedure:

# COMMAND ----------

# Example conversions for detected spatial patterns:
''']

        # Add specific examples based on detected functions
        seen_types = set()

        for func in spatial_functions:
            if func.function_name in seen_types:
                continue
            seen_types.add(func.function_name)

            if "POINT" in func.function_name:
                examples.append('''
# geography::Point(lat, lon, 4326) conversion:
# Original: geography::Point(@latitude, @longitude, 4326)
# Converted: Simply use lat/lon columns directly, or create WKT:
#
# df = df.withColumn("Location", F.concat(
#     F.lit("POINT ("),
#     F.col("Longitude").cast("string"),
#     F.lit(" "),
#     F.col("Latitude").cast("string"),
#     F.lit(")")
# ))
#
# Or parse existing WKT:
# df = parse_wkt_point_columns(df, "PointWKT", "Latitude", "Longitude")
''')

            elif func.function_name == "ST_DISTANCE":
                examples.append('''
# .STDistance() conversion:
# Original: point1.STDistance(point2)
# Converted: haversine_distance(lat1, lon1, lat2, lon2)
#
# df = df.withColumn(
#     "distance_meters",
#     haversine_distance(
#         F.col("worker_lat"), F.col("worker_lon"),
#         F.col("zone_center_lat"), F.col("zone_center_lon")
#     )
# )
#
# Or use the expression version for better performance:
# df = df.withColumn("distance_meters", haversine_distance_expr("lat1", "lon1", "lat2", "lon2"))
''')

            elif func.function_name == "ST_CONTAINS":
                examples.append('''
# .STContains() conversion:
# Original: polygon.STContains(point)
# Converted: Use H3 or Shapely
#
# H3 approach (fast, approximate):
# df = df.withColumn("point_h3", geo_to_h3(F.col("Latitude"), F.col("Longitude"), F.lit(9)))
# df = df.withColumn("in_zone", F.col("point_h3").isin(zone_h3_indices))
#
# Shapely approach (precise, slower):
# df = df.withColumn("in_zone", point_in_polygon(F.col("Latitude"), F.col("Longitude"), F.col("zone_wkt")))
''')

        return '\n'.join(examples)

    def convert_inline(self, sql: str, context: Dict) -> str:
        """
        Convert spatial function calls inline within SQL.

        This provides direct SQL replacements rather than UDF setup.
        """
        result = sql

        # Convert geography::Point to lat/lon handling
        # CASE WHEN ta.latitude IS NOT NULL THEN geography::Point(ta.latitude, ta.longitude, 4326) ELSE NULL END
        # → Keep as separate lat/lon columns

        point_pattern = r"CASE\s+WHEN\s+(\w+\.\w+)\s+IS\s+NOT\s+NULL\s+THEN\s+geography::Point\s*\(\s*([^,]+),\s*([^,]+),\s*\d+\s*\)\s+ELSE\s+NULL\s+END"

        def point_replacement(match):
            condition_col = match.group(1)
            lat_expr = match.group(2).strip()
            lon_expr = match.group(3).strip()
            return f"-- Spatial: Lat={lat_expr}, Lon={lon_expr} (condition: {condition_col} IS NOT NULL)"

        result = re.sub(point_pattern, point_replacement, result, flags=re.IGNORECASE)

        return result
