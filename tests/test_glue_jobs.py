from pathlib import Path


GLUE_JOBS_DIR = Path("glue_jobs")


def test_glue_jobs_directory_exists():
    assert GLUE_JOBS_DIR.exists()


def test_silver_folder_exists():
    assert (GLUE_JOBS_DIR / "silver").exists()


def test_gold_folder_exists():
    assert (GLUE_JOBS_DIR / "gold").exists()


def test_common_folder_exists():
    assert (GLUE_JOBS_DIR / "common").exists()