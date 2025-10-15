from datetime import datetime, timedelta

from airflow.decorators import dag, task

from airflow import settings

DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/ecommerce_dbt"

@dag(
    dag_id = "dags_pipeline",
    default_args = {
        "owner": "data-engg",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=30)
    },
    schedule = timedelta(hours=6), 
    start_date = datetime(2025, 10, 11),
    catchup=False,
    tags = ["dbt", "lakehouse", "iceberg", "analytics"],
    max_active_runs = 1)

def dags_pipeline():

    @task
    def start_pipeline():
        import logging

        logger = logging.getLogger(__name__)

        logger.info("Starting the data pipeline...")

        pipeline_metadata = {
            'pipeline_start_time': datetime.now().isoformat(),
            'dbt_root_dir': DBT_ROOT_DIR,
            'pipeline_id': f"dag_pipeline_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'environment': 'production'
        }

        logger.info(f"Starting pipeline with ID: {pipeline_metadata['pipeline_id']}")
        
        return pipeline_metadata

    @task
    def seed_bronze(pipeline_metadata):
        import logging

        logger = logging.getLogger(__name__)

        logger.info("Seeding bronze....")

        try:
            import sqlalchemy
            from sqlalchemy import create_engine
            from sqlalchemy import text

            engine = create_engine('trino:://trino@trino-coordinator:8080/iceberg/bronze')

            with engine.connect() as connection:
                result = connection.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                bronze_count = result.scalar()

                if bronze_count and bronze_count > 0:
                    logger.info(f"Bronze already seeded with {bronze_count} rows, skipping seeding")
                    return {
                        'status': 'skipped',
                        'layer': 'bronze_seed',
                        'pipeline_id': pipeline_metadata['pipeline_id'],
                        'timestamp': datetime.now().isoformat(),
                        'message': f"Bronze already seeded with {bronze_count} rows"
                    }
        
        except Exception as e:
            logger.info(f"Tables do not exist or error occurred: {e}, proceeding with seeding")
        
        operator = DbtOperator(
            task_id = "seed_bronze_data_internal",
            dbt_root_dir = DBT_ROOT_DIR,
            dbt_command = "seed",
            full_refresh = True
        )

    @task
    def load(data):
        print(f"Loading data: {data}")

    data = extract()
    transformed_data = transform(data)
    load(transformed_data)