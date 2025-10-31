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
        
        if self.target:
            command_args.extend(['--target', self.target])
        
        if self.select:
            command_args.extend(['--select', self.select])

        if self.full_refresh:
            command_args.append('--full-refresh')

        if self.dbt_vars:
            vars_string = ' '.join([f"{key}:{value}" for key, value in self.dbt_vars.items()])
            command_args.extend(['--vars', vars_string])
        
        self.log.info(f"Executing DBT command: {' '.join(command_args)}")
        
        res: dbtRunnerResult = dbtRunner().invoke(command_args)
        
        if res.success:
            self.log.info(f"DBT command executed successfully.")

            if res.result:
                try:
                    for r in res.result:
                        if hasattr(r, 'error') and hasattr(r, 'status'):
                            self.log.info(f"Node {r.node.get.name} : status {r.status}")
                    
                except TypeError:
                    self.log.info("Command completed with result type : {type(res.result).__name__}")
            else:
                self.log.info("DBT command completed with no result.")
        
        else:
            self.log.error("DBT command failed.")
            if res.exception:
                self.log.error(f"Exception: {res.exception}")
            raise AirflowException("DBT command execution failed: {' '.join(command_args)}")