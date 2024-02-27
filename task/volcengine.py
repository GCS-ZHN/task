from typing import Iterable
import yaml
import pandas as pd
import case_convert
import time
import random

from volcengine_ml_platform.openapi import custom_task_client
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from pathlib import Path
from .base import TaskManager, TaskStatus


class VolcengineMLTaskManager(TaskManager):

    _status_map = {
        'Initialized': TaskStatus.PENDING,
        'Staging': TaskStatus.PENDING,
        'Queue': TaskStatus.PENDING,
        'Waiting': TaskStatus.PENDING,
        'Success': TaskStatus.COMPLETED,
        'Failed': TaskStatus.FAILED,
        'Exception': TaskStatus.FAILED,
        'Running': TaskStatus.RUNNING,
        'Killed': TaskStatus.CANCELLED,
        'Killing': TaskStatus.CANCELLED
    }

    _dependency_status_map = {
        'afterok': {
            'continue_waiting': [TaskStatus.PENDING, TaskStatus.RUNNING],
            'start': [TaskStatus.COMPLETED],
            'stop': [TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.UNKNOWN]
        },
        'afternotok': {
            'continue_waiting': [TaskStatus.PENDING, TaskStatus.RUNNING],
            'start': [TaskStatus.FAILED],
            'stop': [TaskStatus.CANCELLED, TaskStatus.COMPLETED, TaskStatus.UNKNOWN]
        },
        'afterany': {
            'continue_waiting': [TaskStatus.PENDING, TaskStatus.RUNNING],
            'start': [TaskStatus.COMPLETED, TaskStatus.FAILED],
            'stop': [TaskStatus.CANCELLED, TaskStatus.UNKNOWN]
        },
        'after': {
            'continue_waiting': [TaskStatus.PENDING],
            'start': [TaskStatus.RUNNING, TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED],
            'stop': [TaskStatus.UNKNOWN]
        }
    }

    def __init__(self, config_file: Path):
        self.task_client = custom_task_client.CustomTaskClient()
        _config = yaml.safe_load(open(config_file))
        self._config = {case_convert.snake_case(k): v for k, v in _config.items()}
        self._task_list = []
        # pending tasks due to dependencies
        self._pendding_tasks = set()
        # cancelled pending tasks due to dependencies will not be satisfied or cancelled manually
        self._pendding_cancelled_tasks = set()
        # tasks failed to submit
        self._submit_error_tasks = set()
        # atomic lock for pendding and submit
        self._atomic_lock = Lock()
        self._submit_executor = ThreadPoolExecutor(
            max_workers=20,
            thread_name_prefix="volcengine-mltask-submit-")
        self._task_id_map = {}

    def _unique_task_id(self):
        """
        Generate a unique task ID.
        style: 20210101-092001-0001 (date-time-random_number)

        Returns
        ----------
        str
            The unique task ID.
        """
        return time.strftime("%Y%m%d-%H%M%S-") + f'{random.randint(0, 9999):04}'

    def _async_submit(
            self, 
            task_id: str,
            name: str, entrypoint_path: str, dependencies: dict = None, **config):
        should_submit = True
        if dependencies is not None and len(dependencies) > 0:
            for condition, dep_task_ids in dependencies.items():
                cond_map = self._dependency_status_map.get(condition)
                if isinstance(dep_task_ids, str):
                    dep_task_ids = [dep_task_ids]
                if not isinstance(dep_task_ids, Iterable):
                    raise ValueError(
                        f"dependencies should be an iterable or str, but got {type(dep_task_ids)}")
                while True:
                   # task cancelled during waiting
                    if task_id not in self._pendding_tasks:
                        should_submit = False
                        break
                    statuses = [self.status(dep_task_id) for dep_task_id in dep_task_ids]
                    if all(status in cond_map['start'] for status in statuses):
                        should_submit = True
                        break
                    if any(status in cond_map['stop'] for status in statuses):
                        should_submit = False
                        break

                    self.log(
                        f'Task {dep_task_ids} are at {statuses}, waiting for {condition} to submit task {task_id}',
                        level='DEBUG')
                    time.sleep(1)
                
                if not should_submit:
                    break

        # make sure atomicity
        with self._atomic_lock:
            if task_id in self._pendding_tasks:
                self._pendding_tasks.remove(task_id)
            # Canceled before submited
            else:
                should_submit = False
            if not should_submit:
                self.log(
                    f'Task {task_id} has been cancelled due to dependencies not satisfied or manually cancelled before submitting',
                    level='DEBUG'
                )
                self._pendding_cancelled_tasks.add(task_id)
                return
            _config = self._config.copy()
            _config.update(config)
            try:
                status = self.task_client.create_custom_task(
                    name=name,
                    entrypoint_path=entrypoint_path,
                    **_config
                )
                real_task_id = status['Result']['Id']
                self._task_id_map[task_id] = real_task_id
            except Exception as e:
                self._submit_error_tasks.add(task_id)
                self.log(
                    f'Failed to submit task {task_id}: {e}',
                    level='ERROR')

    def submit(self, name: str, entrypoint_path:str, dependencies: dict = None, **config) -> str:
        task_id = self._unique_task_id()
        with self._atomic_lock:
            self._pendding_tasks.add(task_id)
            self._task_list.append({
                    "task_id": task_id,
                    "name": name,
                    "entrypoint_path": entrypoint_path
                })
        self._submit_executor.submit(
            self._async_submit, task_id, name, entrypoint_path, dependencies, **config)
        return task_id

    def cancel(self, task_id: str) -> bool:
        try:
            # make sure atomicity
            with self._atomic_lock:
                if task_id in self._pendding_tasks:
                    self._pendding_tasks.remove(task_id)
                    self._pendding_cancelled_tasks.add(task_id)
                    return True
                if task_id in self._pendding_cancelled_tasks or task_id in self._submit_error_tasks:
                    self.log(
                        f'Task {task_id} has been cancelled or submitting failed',
                        level='WARN')
                    return False
                
                real_task_id = self._task_id_map.get(task_id)
                status = self.task_client.stop_custom_task(
                    task_id=real_task_id
                )
                return status['Result']['Id'] == real_task_id
        except Exception as e:
            self.log(
                f'Failed to cancel task {task_id}: {e}',
                level='ERROR')
            return False

    def query(self, task_id: str) -> dict:
        # make sure atomicity
        with self._atomic_lock:
            if task_id in self._pendding_tasks:
                return {
                    'State': 'Queue'
                }
            if task_id in self._pendding_cancelled_tasks:
                return {
                    'State': 'Killed'
                }
            if task_id in self._submit_error_tasks:
                return {
                    'State': 'Failed'
                }
            return self.task_client.get_custom_task(
                task_id=self._task_id_map[task_id]
            )['Result']

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
                status.loc[self._task_id_map[x], 'state'], 
                TaskStatus.UNKNOWN) if x in self._task_id_map else self.status(x))
        return task_list

    def __del__(self):
        self._submit_executor.shutdown(wait=False)
        self.log('VolcengineMLTaskManager is shutdown', level='DEBUG')
