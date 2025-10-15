from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException
from dbt.cli.main import dbtRunner, dbtRunnerResult
import os

class DbtOperator(BaseOperator):
    def __init__(self, 
                 dbt_root_dir: str, 
                 dbt_command: str,
                 target:str = None, 
                 select: str = None,
                 dbt_vars:dict = None,
                 full_refresh: bool = False, 
                 **kwargs):
        
        super().__init__(**kwargs)
        
        self.dbt_root_dir = dbt_root_dir
        self.dbt_command = dbt_command
        self.target = target
        self.select = select
        self.dbt_vars = dbt_vars
        self.full_refresh = full_refresh
        self.runner = dbtRunner()


    def execute(self, context: Context) -> Any :
        if not os.path.isdir(self.dbt_root_dir):
            raise AirflowException(f"DBT root directory '{self.dbt_root_dir}' does not exist or is not a directory.")

        logs_dir = os.path.join(self.dbt_root_dir, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        if not os.path.exists(logs_dir):
            try:
                os.makedirs(logs_dir, mode=0o777)
                self.log.inofo(f"Created logs directory at {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to create logs directory at {logs_dir}: {e}")
                raise AirflowException(f"Failed to create logs directory at {logs_dir}: {e}")
        if not os.access(logs_dir, os.W_OK):
            try:
                os.chmod(logs_dir, 0o777)
                self.log.info(f"Set write permissions for logs directory at {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to set write permissions for logs directory at {logs_dir}: {e}")
                raise AirflowException(f"Failed to set write permissions for logs directory at {logs_dir}: {e}")
        
        if isinstance(self.dbt_command, str):
            command_parts = self.dbt_command.split()
        else:
            command_parts = [self.dbt_command]
        
        command_args = command_parts + [
            '--project-dir', self.dbt_root_dir,
            '--profiles-dir', self.dbt_root_dir,
        ]
        
        
        command_args = [self.dbt_command]
        if self.full_refresh:
            command_args.append('--full-refresh')
        
        result: dbtRunnerResult = dbtRunner().invoke(command_args)
        
        if result.success:
            self.log.info(f"DBT command '{self.dbt_command}' executed successfully.")
        else:
            raise AirflowException(f"DBT command '{self.dbt_command}' failed with errors: {result.error}")