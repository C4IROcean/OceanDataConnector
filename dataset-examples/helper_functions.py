from shapely import wkt
from shapely.geometry import LineString, mapping


def convert_geometry_field(data):
    for item in data:
        try:
            geom = wkt.loads(item["geometry"])
            if geom.geom_type == "Point":
                item["geometry"] = {"type": "Point", "coordinates": [geom.x, geom.y]}
            elif geom.geom_type == "LineString":
                item["geometry"] = {"type": "LineString", "coordinates": [list(coord) for coord in geom.coords]}
            elif geom.geom_type == "MultiLineString":
                item["geometry"] = {
                    "type": "MultiLineString",
                    "coordinates": [[list(coord) for coord in line.coords] for line in geom.geoms],
                }
            elif geom.geom_type == "Polygon":
                item["geometry"] = {"type": "Polygon", "coordinates": [[list(coord) for coord in geom.exterior.coords]]}
        except Exception:
            pass

    return data


def wkt_to_edges(geospatial_query):
    """
    Given a WKT string representing a Polygon, return a list of edges as GeoJSON-like Features.
    """
    # Convert WKT string back to Polygon
    polygon = wkt.loads(geospatial_query)

    coords = list(polygon.exterior.coords)

    lines = [LineString([coords[i], coords[i + 1]]) for i in range(len(coords) - 1)]

    bbox_list = [
        {"type": "Feature", "geometry": mapping(line), "properties": {"edge": i}} for i, line in enumerate(lines)
    ]

    return bbox_list
