"""Four operating zones in Colombo, defined as lat/lon bounding boxes.

The telemetry events carry only lat/lon, as the brief specifies. Turning a
coordinate into a zone is a processing concern, so both the speed layer and
the batch layer import zone_of() from here and stay consistent.
"""
import math

ZONES = [
    {"name": "Fort",        "lat": (6.9260, 6.9480), "lon": (79.8360, 79.8580)},
    {"name": "Kollupitiya", "lat": (6.9000, 6.9260), "lon": (79.8400, 79.8650)},
    {"name": "Borella",     "lat": (6.9000, 6.9300), "lon": (79.8650, 79.8950)},
    {"name": "Dehiwala",    "lat": (6.8380, 6.8760), "lon": (79.8540, 79.8880)},
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