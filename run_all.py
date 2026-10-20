"""依次运行所有示例和测试：python3 run_all.py"""

import glob
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
scripts = sorted(glob.glob(os.path.join(here, "examples", "[0-9]*.py"))) + [os.path.join(here, "tests", "test_core.py")]
failed = []
for script in scripts:
    name = os.path.relpath(script, here)
    print(f"\n{'=' * 20} {name} {'=' * 20}", flush=True)
    if subprocess.call([sys.executable, script], timeout=120) != 0:
        failed.append(name)
print("\n" + ("ALL PASSED" if not failed else f"FAILED: {failed}"))
sys.exit(1 if failed else 0)
