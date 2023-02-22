import os
import subprocess
import webbrowser

import angreal
from angreal.integrations.venv import VirtualEnv, venv_required

venv_location = os.path.join(angreal.get_root(),'..','.venv')
cwd = os.path.join(angreal.get_root(), '..')

venv_python = VirtualEnv(venv_location).ensure_directories.env_exe

@venv_required(venv_location)
@angreal.command(name='run-tests', about="run our test suite. default is unit tests only")
@angreal.argument(name="integration", long="integration", short='i', takes_value=False, help="run integration tests only")
@angreal.argument(name="full", long="full", short='f', takes_value=False, help="run integration and unit tests")
@angreal.argument(name="open", long="open", short='o', takes_value=False, help="open results in web browser")
def run_tests(integration=False,full=False,open=False):

    if full:
        integration=False

    output_file = os.path.realpath(os.path.join(cwd,'htmlcov','index.html'))

    if integration:
        subprocess.run(f" {venv_python} -m pytest -vvv --cov=airflow_provider_kafka --cov-report html --cov-report term tests/integration",shell=True, cwd=cwd)
    if full:
        subprocess.run(f"{venv_python} -m pytest -vvv --cov=airflow_provider_kafka --cov-report html --cov-report term tests/",shell=True, cwd=cwd)
    if not integration and not full:
        subprocess.run(f" {venv_python} -m pytest -vvv --cov=airflow_provider_kafka --cov-report html --cov-report term tests/unit",shell=True, cwd=cwd)

    if open:
        webbrowser.open_new('file://{}'.format(output_file))


@venv_required(venv_location)
@angreal.command(name='static-tests', about="run static analyses on our project")
@angreal.argument(name="open", long="open", short='o', takes_value=False, help="open results in web browser")
def static(open):
    subprocess.run(
        (
            f"{venv_python} -m  mypy airflow_provider_kafka --ignore-missing-imports --html-report typing_report"
        ),
        shell=True,
        cwd=cwd
    )

    if open:
        webbrowser.open(f'file:://{os.path.join(cwd,"typing_report","index.html")}')


@venv_required(venv_location)
@angreal.command(name='lint', about="lint our project")
def lint(open):
    subprocess.run(
        (
        "pre-commit run --all-files"
        ),
        shell=True,
        cwd=cwd
    )

    if open:
        webbrowser.open(f'file:://{os.path.join(cwd,"typing_report","index.html")}')
