from dagster import (
    Definitions,
    load_assets_from_package_module,
    define_asset_job,
    SkipReason,
    RunRequest,
    SensorDefinition
)
from ndl_core_data_pipeline import assets as assets_package
from .resources.api_client import RateLimitedApiClient

from ndl_core_data_pipeline.assets.gov_uk.assets import gov_uk_batches, gov_uk_search_index, gov_uk_process_batch
from ndl_core_data_pipeline.assets.data_gov_uk.assets import (
    data_gov_discover_categories,
    data_gov_process_category,
    data_gov_categories,
)


all_assets = load_assets_from_package_module(
    package_module=assets_package
)

batch_job = define_asset_job("gov_uk_batch_job", selection=[gov_uk_process_batch])
data_gov_job = define_asset_job("data_gov_batch_job", selection=[data_gov_process_category])


def gov_uk_sensor(context):
    """
    Dynamic partitions based fail-safe crawling sensor.
    :param context:
    :return:
    """
    # 1. Check if the search index asset has new materializations
    # (Implementation simplified for brevity - usually checks cursor)

    # 2. Get the current list of keys in the partition def
    keys = context.instance.get_dynamic_partitions("gov_uk_search_batches")

    if not keys:
        return SkipReason("No partitions found yet")

    return RunRequest(
        run_key=None,
        job_name="gov_uk_batch_job",
        partition_keys=keys
    )


def data_gov_sensor(context):
    """
    Sensor that triggers the data_gov batch job for all dynamic partitions (categories).
    """
    # Use the dynamic partitions definition name from the asset module
    keys = context.instance.get_dynamic_partitions(data_gov_categories.name)

    if not keys:
        return SkipReason("No data_gov partitions found yet")

    return RunRequest(
        run_key=None,
        job_name="data_gov_batch_job",
        partition_keys=keys,
    )

defs = Definitions(
    assets=all_assets,
    jobs=[batch_job, data_gov_job],
    sensors=[
        SensorDefinition(name="trigger_gov_uk_batches", evaluation_fn=gov_uk_sensor, job=batch_job),
        SensorDefinition(name="trigger_data_gov_categories", evaluation_fn=data_gov_sensor, job=data_gov_job),
    ],
    resources={
        "api_gov_uk": RateLimitedApiClient(base_url="https://www.gov.uk", rate_limit_per_second=10.0),
        "api_data_gov": RateLimitedApiClient(base_url="https://data.gov.uk", rate_limit_per_second=None),
        "api_legislation": RateLimitedApiClient(base_url="https://www.legislation.gov.uk", rate_limit_per_second=None),
    }
)