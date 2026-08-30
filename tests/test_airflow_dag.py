from pathlib import Path


DAG_FILE = Path("airflow/dags/shopify_pipeline_dag.py")


def test_dag_file_exists():
    assert DAG_FILE.exists()


def test_dag_contains_definition():
    content = DAG_FILE.read_text()

    assert "DAG(" in content
    assert "shopify_data_pipeline" in content


def test_glue_job_operator_used():
    content = DAG_FILE.read_text()

    assert "GlueJobOperator" in content