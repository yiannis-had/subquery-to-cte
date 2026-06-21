import importlib.util
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

spec = importlib.util.spec_from_file_location(
    "subq_to_cte", project_root / "subq-to-cte.py"
)
mod = importlib.util.module_from_spec(spec)
sys.modules["subq_to_cte"] = mod
spec.loader.exec_module(mod)
