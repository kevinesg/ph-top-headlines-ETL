from subflows.etl_web_to_gcs import etl_web_to_gcs
from subflows.etl_gcs_to_gbq import etl_gcs_to_gbq
from prefect import flow


@flow(name='ph-news main flow', log_prints=True)
def main_flow() -> None:

    subflow_1 = etl_web_to_gcs()
    subflow_2 = etl_gcs_to_gbq(wait_for=[subflow_1])


if __name__ == '__main__':
    main_flow()