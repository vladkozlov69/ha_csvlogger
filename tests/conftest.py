"""Load this standalone integration as a package for unit tests."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys


INTEGRATION_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "csvlogger"

if PACKAGE_NAME not in sys.modules:
    spec = spec_from_file_location(
        PACKAGE_NAME,
        INTEGRATION_ROOT / "__init__.py",
        submodule_search_locations=[str(INTEGRATION_ROOT)],
    )
    if spec is None or spec.loader is None:
        raise ImportError("Could not load the CSVLogger integration package")
    module = module_from_spec(spec)
    sys.modules[PACKAGE_NAME] = module
    spec.loader.exec_module(module)
