from fastapi import FastAPI
from hip_data_tools.hipages.version_tracking import VersionTracker
from hip_data_tools.hipages.version_tracking \
    import register_method_for_version_tracking

MULTIPLIER = 5

app = FastAPI()

versions = VersionTracker()
# Import all of our static versions
versions.add_versions_from_json_file("version_tracking.json")

# Add some configuration tracking to our version control
versions.add_string_to_version_tracking("multiplier_value", MULTIPLIER)

@register_method_for_version_tracking
@app.post("/")
def multiplier_endpoint(number_in : int):
    """
    Simple endpoint which takes a number, and multiplies it by a fixed amount
    :param number_in (int): some incoming number
    :return (dict): Returned is the result of the multiplication and the
    relevant versioning information
    """
    return {'multiplied_result': number_in,
            'versions': versions.get_version_dict()}



