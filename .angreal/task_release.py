import os
import subprocess

import angreal
from angreal.integrations.venv import venv_required

venv_location = os.path.join(angreal.get_root(),'..','.venv')
cwd = os.path.join(angreal.get_root(), '..')

@venv_required(venv_location)
@angreal.command(name="dist", about="build your project for distribution")
def run_tests():

    subprocess.run(
        (
        "python -m build --sdist --wheel --outdir dist/;"
        "pip install airflow_provider_kafka --no-index --find-links dist --force-reinstall"
        ),
        shell=True,
        cwd=cwd
    )
