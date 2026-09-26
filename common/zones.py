"""Four operating zones in Colombo, defined as lat/lon bounding boxes.

The telemetry events carry only lat/lon, as the brief specifies. Turning a
coordinate into a zone is a processing concern, so both the speed layer and
the batch layer import zone_of() from here and stay consistent.

The four boxes TILE the operating area with no gaps: an earlier version left
48% of the area uncovered, so nearly every vehicle was reported in a zone
called "Unknown" and earnings could not be attributed to an area.
"""
import math

# Operating area, split into quadrants at the mid lat/lon.
LAT_MIN, LAT_MAX = 6.8380, 6.9480
LON_MIN, LON_MAX = 79.8360, 79.8950
LAT_MID = (LAT_MIN + LAT_MAX) / 2      # 6.8930
LON_MID = (LON_MIN + LON_MAX) / 2      # 79.8655

ZONES = [
    {"name": "Fort",        "lat": (LAT_MID, LAT_MAX), "lon": (
        LON_MIN, LON_MID)},  # north-west
    {"name": "Borella",     "lat": (LAT_MID, LAT_MAX), "lon": (
        LON_MID, LON_MAX)},  # north-east
    {"name": "Kollupitiya", "lat": (LAT_MIN, LAT_MID), "lon": (
        LON_MIN, LON_MID)},  # south-west
    {"name": "Dehiwala",    "lat": (LAT_MIN, LAT_MID), "lon": (
        LON_MID, LON_MAX)},  # south-east
]

ZONE_NAMES = [z["name"] for z in ZONES]


def zone_center(name: str):
    z = next(z for z in ZONES if z["name"] == name)
    return ((z["lat"][0] + z["lat"][1]) / 2, (z["lon"][0] + z["lon"][1]) / 2)


def zone_of(lat: float, lon: float) -> str:
    """Return the zone containing the point, else the nearest zone centre."""
    for z in ZONES:
        if z["lat"][0] <= lat <= z["lat"][1] and z["lon"][0] <= lon <= z["lon"][1]:
            return z["name"]
    best, best_d = "Unknown", float("inf")
    for z in ZONES:
        clat, clon = zone_center(z["name"])
        d = math.hypot(lat - clat, lon - clon)
        if d < best_d:
            best, best_d = z["name"], d
    return best


def km_to_degrees(km: float) -> float:
    """Rough conversion near the equator: 1 degree is about 111 km."""
    return km / 111.0
