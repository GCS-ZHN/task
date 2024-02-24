import yaml
import pandas as pd
import case_convert

from volcengine_ml_platform.openapi import custom_task_client
from .base import TaskManager, TaskStatus


class VolcengineMLTaskManager(TaskManager):

    _status_map = {
        'Success': TaskStatus.COMPLETED,
        'Failed': TaskStatus.FAILED,
        'Staging': TaskStatus.PENDING,
        'Queue': TaskStatus.PENDING,
        'Waiting': TaskStatus.PENDING,
        'Running': TaskStatus.RUNNING,
        'Killed': TaskStatus.CANCELLED,
        'Killing': TaskStatus.CANCELLED
    }

    def __init__(self):
        self.task_client = custom_task_client.CustomTaskClient()
        self.config = yaml.safe_load(open("config/volc_mltask.yaml"))
        self.config = {case_convert.snake_case(k): v for k, v in self.config.items()}
        self._task_list = []

    def submit(self, name: str, entrypoint_path:str, **config) -> str:
        config.update(self.config)
        status = self.task_client.create_custom_task(
            name=name,
            entrypoint_path=entrypoint_path,
            **config
        )
        task_id = status['Result']['Id']
        self._task_list.append({
            "task_id": task_id,
            "name": name,
            "entrypoint_path": entrypoint_path
        
        })
        return task_id

    def cancel(self, task_id: str) -> bool:
        try:
            status = self.task_client.stop_custom_task(
                task_id=task_id
            )
            return status['Result']['Id'] == task_id
        except Exception as e:
            print(e)
            return False

    def query(self, task_id: str) -> dict:
        task_info = self.task_client.get_custom_task(
            task_id=task_id
        )['Result']
        return task_info

    def status(self, task_id: str) -> TaskStatus:
        task_info = self.query(task_id)
        status = task_info['State']
        return self._status_map.get(
            status, TaskStatus.UNKNOWN)

    def list(self) -> pd.DataFrame:
        status = self.task_client.list_custom_tasks()
        status = pd.DataFrame(status['Result']['List'])
        status.rename(columns=case_convert.snake_case, inplace=True)
        status.set_index('id', inplace=True)
        task_list = pd.DataFrame(self._task_list, columns=['task_id', 'name', 'entrypoint_path'])
        task_list.set_index('task_id', inplace=True)
        task_list['status'] = task_list.index.map(
            lambda x: self._status_map.get(
                status.loc[x, 'state'], TaskStatus.UNKNOWN))
            
        return task_list
