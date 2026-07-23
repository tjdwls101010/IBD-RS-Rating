"""CI dependency contract tests."""

from importlib import import_module, metadata
from pathlib import Path
import tomllib


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = PROJECT_ROOT / "requirements.lock"
WORKFLOW_DIR = PROJECT_ROOT / ".github" / "workflows"


def _normalized(name):
    return name.lower().replace("_", "-")


def _lock_versions():
    versions = {}
    for line in LOCK_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        assert " @ " not in line
        assert "==" in line
        name, version = line.split("==", 1)
        assert name
        assert version
        versions[_normalized(name)] = version
    return versions


def test_lock_file_pins_runtime_dependencies_to_installed_versions():
    versions = _lock_versions()

    for package_name in ("pandas", "yfinance", "finvizfinance", "psycopg2-binary"):
        assert package_name in versions
        assert versions[package_name] == metadata.version(package_name)


def test_locked_environment_imports_engine_and_postgres_modules():
    import_module("ibd_rs.cli")

    for module_name in ("pandas", "yfinance", "finvizfinance", "psycopg2"):
        import_module(module_name)


def test_workflows_install_locked_dependencies_and_local_package():
    for workflow_name in ("daily_update.yml", "init.yml"):
        workflow = (WORKFLOW_DIR / workflow_name).read_text(encoding="utf-8")

        assert "requirements.lock" in workflow
        assert "python -m pip install -r requirements.lock" in workflow
        assert "python -m pip install --no-deps ." in workflow
        assert 'pip install ".[engine,pg]"' not in workflow
        assert 'pip install ".[pg]"' not in workflow


def test_distribution_dependencies_stay_loose():
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    optional = pyproject["project"]["optional-dependencies"]

    engine_and_pg = optional["engine"] + optional["pg"]
    assert "pandas>=2.0" in optional["engine"]
    assert "yfinance>=0.2.30" in optional["engine"]
    assert "finvizfinance>=1.3.0" in optional["engine"]
    assert "psycopg2-binary>=2.9" in optional["pg"]
    assert all("==" not in dependency for dependency in engine_and_pg)

def test_reusable_test_workflow_runs_pytest_on_locked_deps():
    test_wf = (WORKFLOW_DIR / "test.yml").read_text(encoding="utf-8")

    assert "workflow_call" in test_wf
    assert "pytest" in test_wf
    assert "requirements.lock" in test_wf
    # matrix must exercise the requires-python floor (3.11) and a current interpreter
    assert "3.11" in test_wf
    assert "3.12" in test_wf


def test_ci_workflow_runs_tests_on_push_and_pull_request():
    ci = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")

    assert "push" in ci
    assert "pull_request" in ci
    # CI delegates to the shared test workflow rather than duplicating it
    assert "./.github/workflows/test.yml" in ci


def test_publish_is_gated_on_a_green_test_job_and_version_match():
    publish = (WORKFLOW_DIR / "publish.yml").read_text(encoding="utf-8")

    # publish job cannot run without a passing test job (the shared workflow)
    assert "needs: test" in publish
    assert "./.github/workflows/test.yml" in publish
    # build smoke and a release-tag == pyproject-version assertion
    assert "python -m build" in publish
    assert "GITHUB_REF_NAME" in publish
