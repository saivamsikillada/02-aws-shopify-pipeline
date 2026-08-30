from awsgluedq.transforms import EvaluateDataQuality


DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""


def run_data_quality(dynamic_frame, evaluation_context):
    """
    Runs AWS Glue Data Quality checks on the given DynamicFrame.
    """

    EvaluateDataQuality().process_rows(
        frame=dynamic_frame,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": evaluation_context,
            "enableDataQualityResultsPublishing": True
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL"
        }
    )