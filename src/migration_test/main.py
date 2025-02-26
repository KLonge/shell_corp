import datetime as dt

from unmind_data_stack.misc_scripts.snowflake_migration.models import (
    ComparisonResult,
)
from unmind_data_stack.misc_scripts.snowflake_migration.utils.comparison import (
    compare_snowflake_tables,
)
from unmind_data_stack.misc_scripts.snowflake_migration.utils.results import (
    comparison_results_to_csv,
)
from unmind_data_stack.models.dbt import DBTModelInfo
from unmind_data_stack.resources.dbt_manifest import (
    DBTManifestResource,
    create_dbt_manifest_resource,
)
from unmind_data_stack.resources.snowflake import (
    SnowflakeResource,
    create_snowflake_resource,
)


def main(folder_path: str) -> None:
    count: int = 0
    comparison_results: list[ComparisonResult] = []

    for model_name, model in PROD_DBT_MODELS.items():
        if model_name in [
            "v_dim_content_tool_current",
            "fct_typeform_answer_submissions_vault",
            "obt_talk_connect__booking_reason_metrics",
        ]:
            continue

        table_name: str = model.name

        if model.unique_key is None:
            raise ValueError(f"Model {table_name} has no unique key")

        unique_key: list[str] = [col.upper() for col in model.unique_key]

        snowflake_client: SnowflakeResource = create_snowflake_resource()

        comparison_result: ComparisonResult = compare_snowflake_tables(
            snowflake_client=snowflake_client,
            table1_name=f"PROD.{table_name}",
            table2_name=f"REDSHIFT_PROD.{table_name}",
            primary_key=unique_key,
            timestamp_range_exclude=(
                dt.datetime(2024, 12, 1),
                dt.datetime(2024, 12, 31),
            ),
            value_tolerance=0.20,
            row_tolerance=0.01,
            exclude_columns=[
                "LATEST_OS_VERSION",
                "LATEST_TIMEZONE",
                "LATEST_PLATFORM",
                "LATEST_OS_NAME",
                "LATEST_UPDATE_TIMESTAMP",
                "LATEST_DEVICE_TYPE",
                "LATEST_DEVICE_FAMILY",
                "LATEST_LOCALE",
                "USER_LATEST_TIMEZONE",
                "USER_LATEST_PLATFORM",
                "USER_LATEST_OS_NAME",
                "USER_LATEST_OS_VERSION",
                "USER_LATEST_DEVICE_TYPE",
                "USER_LATEST_DEVICE_FAMILY",
                "USER_LATEST_LOCALE",
                "MEDIAN_SESSION_INTERVAL_USER_LVL",
                "AVG_SESSION_INTERVAL_USER_LVL",
                "SESSION_START_AT_LOCAL",
                "SESSION_NUMBER",
                "SESSION_END_AT_LOCAL",
                "SESSION_INTERVAL_DAYS",
                "IS_VALID_USER_LATEST_TIMEZONE",
                "FOLLOWUP_SESSIONS_AFTER_FIRST_RECOMMENDATION",
                "DISPATCH_ID",
                "EVENTTIMESTAMP_LOCALTIME",
                "URL",
                "CLIENT_ONBOARDING_KEY",
                "UPDATED_AT_UTC",
            ],
            trim_strings=True,
        )
        comparison_result.model_name = model_name
        comparison_results.append(comparison_result)
        count += 1
        print(f"\n{count}/{TOTAL_MODEL_COUNT} MODELS COMPARED")

    comparison_results_to_csv(
        folder_path=folder_path, comparison_results=comparison_results
    )

    failed_models: list[ComparisonResult] = [
        result for result in comparison_results if not result.passed
    ]

    if failed_models:
        print(f"\n� FAILED MODELS: {len(failed_models)}/{TOTAL_MODEL_COUNT}")

        for failed_model in failed_models:
            print(f"⚠️ {failed_model.model_name}")
            print("\n")
            print(f"📊 {failed_model.failed_row_perc * 100:.2f}% OF ROWS FAILED")
            print("\n")
            # print("🔍 SQL to inspect differences:")
            # print(f"{failed_model.comparison_sql}")
            # print("\n")
            print("-" * 100)
            print("\n")
    else:
        print("\n✅ ALL MODELS PASSED")


if __name__ == "__main__":
    DBT_MANIFEST_CLIENT: DBTManifestResource = create_dbt_manifest_resource()

    # FOLDER_PATH: str = "marts/product/workplace_index"
    # FOLDER_PATH: str = "marts/product/lookup_tables"
    # FOLDER_PATH: str = "marts/product/engagement"
    # FOLDER_PATH: str = "marts/product/content_metadata"
    # FOLDER_PATH: str = "marts/product/core"
    # FOLDER_PATH: str = "marts/product/unmind_talk"
    # FOLDER_PATH: str = "marts/typeform"
    # FOLDER_PATH: str = "marts/product_support"
    # FOLDER_PATH: str = "marts/webinar"
    # FOLDER_PATH: str = "marts/product/adoption"
    # FOLDER_PATH: str = "marts/product/nova"
    # FOLDER_PATH: str = "marts/admin_dashboard_production_models"
    # FOLDER_PATH: str = "marts/braze"
    FOLDER_PATH: str = "marts"
    # FOLDER_PATH: str = "marts/product_support"

    # PROD_DBT_MODELS: dict[str, DBTModelInfo] = DBT_MANIFEST_CLIENT.get_all_models_info(
    #     folder_path=FOLDER_PATH
    # )

    # PROD_DBT_MODELS: dict[str, DBTModelInfo] = DBT_MANIFEST_CLIENT.get_all_models_info(
    #     model_names=[
    #         # "agg_extended_monthly_retention",
    #         # "fct_series_segment_completed",
    #         # "fct_tool_session",
    #         "obt_nova__session_slug_lvl_metrics",
    #     ]
    # )

    PROD_DBT_MODELS: dict[str, DBTModelInfo] = DBT_MANIFEST_CLIENT.get_all_models_info(
        model_names=[
            "dim_tool_current",
        ]
    )

    TOTAL_MODEL_COUNT: int = len(PROD_DBT_MODELS)

    main(folder_path=FOLDER_PATH)
