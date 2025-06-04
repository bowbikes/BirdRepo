import geopy.distance
import math
from typing import List, Tuple
import pandas as pd
from math import radians, sin, cos, atan2, sqrt, asin, acos, pi, degrees
from datetime import datetime

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def areSameFlock(coords_1,coords_2,dist_threshold):
    if geopy.distance.geodesic(coords_1,coords_2).miles <dist_threshold:
        return True
    else:
        return False

def getFlockNumber(bird, flocks):
    for idx, flock in enumerate(flocks, start=1):
        if bird in flock:
            return idx
    return 0

def reorder(lst):
    o_lst = []
    for f in lst:
        if f[0] > f[1]:
            o_lst.append([f[1],f[0]])
        else:
            o_lst.append(f)
    return(o_lst)


def is_consecutive(yr1,yr2,s1,s2):
    if yr1 == yr2 and s1 != s2 and s2 == 'summer':
        return True
    elif yr1 == yr2-1 and s1 != s2 and s2 == 'winter':
        return True
    else:
        return False

def geographic_midpoint(
    points: List[Tuple[float, float, float]]
) -> Tuple[float, float]:

    x_sum = y_sum = z_sum = 0.0
    total_w = 0.0

    for lat_deg, lon_deg, w in points:
        # to radians
        lat = math.radians(lat_deg)
        lon = math.radians(lon_deg)

        # to Cartesian on unit sphere
        x = math.cos(lat) * math.cos(lon)
        y = math.cos(lat) * math.sin(lon)
        z = math.sin(lat)

        # accumulate weighted Cartesian coords
        x_sum += x * w
        y_sum += y * w
        z_sum += z * w
        total_w += w

    if total_w == 0:
        raise ValueError("Total weight is zero; cannot compute midpoint.")

    # weighted average
    x_avg = x_sum / total_w
    y_avg = y_sum / total_w
    z_avg = z_sum / total_w

    # ensure not the zero vector
    if abs(x_avg) < 1e-9 and abs(y_avg) < 1e-9 and abs(z_avg) < 1e-9:
        raise ValueError("Midpoint lies at the Earth's center (zero vector).")

    # back to spherical coords
    lon_mid = math.atan2(y_avg, x_avg)
    hyp = math.hypot(x_avg, y_avg)
    lat_mid = math.atan2(z_avg, hyp)

    # to degrees
    return math.degrees(lat_mid), math.degrees(lon_mid)


def distance_components(lat1, lon1, lat2, lon2):
    # Earth’s radius in km
    R = 6371.0
    # convert degrees to radians
    p = pi / 180.0
    lat1_r, lat2_r = lat1 * p, lat2 * p
    lon1_r, lon2_r = lon1 * p, lon2 * p

    # haversine for total distance
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = 0.5 - cos(dlat)/2 + cos(lat1_r) * cos(lat2_r) * (1 - cos(dlon)) / 2
    total_dist = 2 * R * asin(sqrt(a))

    # north-south component (signed; positive if lat2 > lat1)
    ns_dist = R * dlat

    # east-west component (signed; positive if lon2 > lon1)
    # scale by cos(mean latitude) to get actual km along that parallel
    mean_lat = (lat1_r + lat2_r) / 2
    ew_dist = R * cos(mean_lat) * dlon

    return total_dist, ew_dist, ns_dist

def _central_angle(lat1, lon1, lat2, lon2):
    """
    Returns the central angle (in radians) between two lat/lon points.
    """
    φ1, φ2 = radians(lat1), radians(lat2)
    Δφ = radians(lat2 - lat1)
    Δλ = radians(lon2 - lon1)
    a = sin(Δφ/2)**2 + cos(φ1)*cos(φ2)*sin(Δλ/2)**2
    return 2 * asin(sqrt(a))

def _bearing(lat1, lon1, lat2, lon2):
    """
    Returns the initial bearing (in radians) from point 1 to point 2.
    """
    φ1, φ2 = radians(lat1), radians(lat2)
    Δλ = radians(lon2 - lon1)
    y = sin(Δλ) * cos(φ2)
    x = cos(φ1)*sin(φ2) - sin(φ1)*cos(φ2)*cos(Δλ)
    return atan2(y, x)

def bearing_degrees(lat1, lon1, lat2, lon2) -> float:
    """
    Returns the initial bearing from point1→point2, as a degree in [0,360).
    """
    # 1) get bearing in radians
    rad = _bearing(lat1, lon1, lat2, lon2)
    # 2) convert to degrees
    deg = math.degrees(rad)
    # 3) normalize to [0, 360)
    return (deg + 360) % 360

def min_distance_to_path(lat1, lon1, lat2, lon2, lat3, lon3):
    """
    Returns the minimum distance (in kilometers) from point 3 to the
    segment between point 1 and point 2 on the Earth’s surface.
    """
    R = 6371.0  # Earth radius in km

    # angular distances and bearings
    δ13 = _central_angle(lat1, lon1, lat3, lon3)
    δ12 = _central_angle(lat1, lon1, lat2, lon2)
    θ12 = _bearing(lat1, lon1, lat2, lon2)
    θ13 = _bearing(lat1, lon1, lat3, lon3)

    # cross-track angular distance
    δxt = asin(sin(δ13) * sin(θ13 - θ12))
    xt_dist = abs(δxt) * R

    # along-track angular distance from point 1
    δat = acos(cos(δ13) / cos(δxt))
    at_dist = δat * R

    # if closest point falls outside the [1→2] segment, use endpoint distances
    if δat < 0 or δat > δ12:
        # distance from 3 to 1
        d31 = δ13 * R
        # distance from 3 to 2
        d32 = _central_angle(lat2, lon2, lat3, lon3) * R
        return min(d31, d32)
    else:
        return xt_dist
