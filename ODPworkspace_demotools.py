import pydeck
import pydeck as pdk
from ipyleaflet import DrawControl, Map, TileLayer
from IPython.display import display
from ipywidgets import Textarea, VBox, widgets

## EDIT COMMENT

## EDIT COMMENT number 2

# Your Mapbox access token
mapbox_token = (
    "pk.eyJ1Ijoib2NlYW5kYXRhZm91bmRhdGlvbiIsImEiOiJjazk5bGxpNWkwYWU1M2Vya3hkcHh4czdrIn0.yf7kIiPfDNE7KP9_9wTN6A"
)

# Your custom Mapbox style URL (formatted for TileLayer)
mapbox_style_url_ipyleaflet = f"https://api.mapbox.com/styles/v1/oceandatafoundation/clwg6xklg00an01pcgmeufjxq/tiles/256/{{z}}/{{x}}/{{y}}@2x?access_token={mapbox_token}"  # noqa: E501

mapbox_token = (
    "pk.eyJ1Ijoib2NlYW5kYXRhZm91bmRhdGlvbiIsImEiOiJjazk5bGxpNWkwYWU1M2Vya3hkcHh4czdrIn0.yf7kIiPfDNE7KP9_9wTN6A"
)
mapbox_style_url = "mapbox://styles/oceandatafoundation/clwg6xklg00an01pcgmeufjxq"


def mapOQS():
    # Initialize a map
    m = Map(
        center=(52.204793, 0.121558),
        zoom=2,
        scroll_wheel_zoom=True,
        layers=[TileLayer(url=mapbox_style_url_ipyleaflet)],
    )
    m.layout = widgets.Layout(width="700px", height="450px")

    # Initialize the drawing control
    draw_control = DrawControl(
        polygon={"shapeOptions": {"color": "#03FFD1", "fillColor": "#03FFD1", "fillOpacity": 0.4, "opacity": 1.0}},
        polyline={},
        circle={},
        rectangle={},
        circlemarker={},
    )

    # Create a TextArea widget to display the coordinates
    coordinates_output = Textarea(
        value="Polygon coordinates will be displayed here",
        placeholder="Polygon coordinates will be displayed here",
        description="Coordinates:",
        layout={"width": "700px", "height": "60px"},
    )

    # Function to handle the drawing events
    def handle_draw(self, action, geo_json):
        if action == "created" or action == "edited":
            coordinates = geo_json["geometry"]["coordinates"]
            coordinates_wktstring = f"POLYGON(({', '.join([f'{lon:.3f} {lat:.3f}' for lon, lat in coordinates[0]])}))"
            coordinates_output.value = coordinates_wktstring

    # Link the drawing control to the handle_draw function
    draw_control.on_draw(handle_draw)
    m.add_control(draw_control)

    # Display the map and the coordinates output
    display(VBox([m, coordinates_output]))

    # Return the coordinates_output widget
    return coordinates_output


# Colors to use for features
colors = [(3, 255, 209), (255, 216, 11), (157, 89, 244), (254, 119, 76)]  # Cyan, Yellow, Purple, Orange


def pydeck_plot(geojson_lists, pickable=False, lat=55, lon=7, zoom=3, **layerparams):
    """
    Creates a Pydeck GeoJson plot based on multiple GeoJSON objects.

    Args:
        geojson_lists (list of dict): List of input GeoJSON objects.
        pickable (bool): Whether the layers should be pickable. Default is False.
        latitude (float): Latitude of the initial view state. Default is 55.
        longitude (float): Longitude of the initial view state. Default is 7.
        zoom (int): Zoom level of the initial view state. Default is 3.
        **layerparams: Additional parameters to pass to the layer.

    Returns:
        pdk.Deck: Combined Pydeck GeoJson plot.
    """
    layers = []
    for i, geojson_list in enumerate(geojson_lists):
        # Default layer properties
        default_layer_props = {
            "lineWidthMinPixels": 1,
            "pointRadiusMinPixels": 1,
            "get_line_color": colors[i % len(colors)],  # Assign color based on index, wrap around if needed
            "get_fill_color": colors[i % len(colors)],  # Assign color based on index, wrap around if needed
            "pickable": pickable,
        }

        # Update default properties with user-provided properties
        default_layer_props.update(layerparams)

        layer = pdk.Layer("GeoJsonLayer", data=geojson_list, **default_layer_props)
        layers.append(layer)

    initial_view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=zoom)

    r = pdk.Deck(
        layers=layers,
        initial_view_state=initial_view_state,
        api_keys={"mapbox": mapbox_token},
        map_provider="mapbox",
        map_style=mapbox_style_url,
    )
    return r


# Example usage
# geojson_list = [...]  # List of GeoJSON objects
# deck = pydeck_plot(geojson_list, pickable=True, lineWidthScale=2, pointRadiusScale=2)
# deck.show()  # Use in a Jupyter notebook to display the map
