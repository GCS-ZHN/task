from typing import Iterable
import yaml
import tos
import pandas as pd
import case_convert
import time
import random
import json

from io import StringIO
from volcengine_ml_platform.openapi import custom_task_client
from concurrent.futures import ThreadPoolExecutor
from threading import RLock, Thread, current_thread
from pathlib import Path
from .base import TaskManager, TaskStatus
from .utils import sha256sum, sha256sum_str
from contextlib import contextmanager


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
        self._task_client = custom_task_client.CustomTaskClient()
        self._credentials = self._task_client.service_info.credentials
        self._tos_client = tos.TosClientV2(
            ak=self._credentials.ak,
            sk=self._credentials.sk,
            endpoint=f'tos-{self._credentials.region}.volces.com',
            region=self._credentials.region
        )
        _config = yaml.safe_load(open(config_file))
        self._config = {case_convert.snake_case(k): v for k, v in _config.items()}
        self._bucket_name = self._config['bucket']
        del self._config['bucket']
        self._task_list = pd.DataFrame(columns=['task_id', 'name', 'entrypoint_path', 'status', 'dependencies'])
        self._task_list.set_index('task_id', inplace=True)
        # pending tasks due to dependencies
        self._pendding_tasks = set()
        # cancelled pending tasks due to dependencies will not be satisfied or cancelled manually
        self._pendding_cancelled_tasks = set()
        # tasks failed to submit
        self._submit_error_tasks = set()
        # atomic lock for pendding and submit
        self._atomic_lock = RLock()
       # Hash tag for current mananager
        self._hash_tag = self._unique_task_id()
        self.log(f'Task manager {self._hash_tag} created')
        self._submit_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"volcengine-mltask-submit-{self._hash_tag}")
        # virtural ID Mapping
        self._task_id_map = {}
        # max submited real running/pendding tasks
        self._max_real_submited_tasks = 64
        # watch thread for task
        def _watch():
            while True:
                with self.synchronize():
                    info = {
                        'total': len(self._task_list),
                        'pending': len(self._pendding_tasks),
                        'pending_cancel': len(self._pendding_cancelled_tasks),
                        'submit_error': len(self._submit_error_tasks),
                        'submited': len(self._task_id_map)
                    }
                    msg = ' '.join(f'{k}:{v}' for k, v in info.items())
                    if info['total'] > 0:
                        self.log(msg, level='DEBUG')
                time.sleep(10)

        _watch_thread = Thread(
            name='volcengine-mltask-watch-' + self._hash_tag,
            target=_watch,
            daemon=True
        )
        _watch_thread.start()

        # sync task info from remote
        self.list()
        def _sync():
            while True:
                self.list()
                statistics = self._task_list['status'].value_counts().to_dict()
                statistics = ' '.join(f'{k}:{v}' for k, v in statistics.items())
                self.log(
                    f'Sync remote task info: {statistics}',
                    level='DEBUG'
                )
                self._task_list.to_csv(f'/tmp/task_{self._hash_tag}.csv')
                time.sleep(20)
        
        _sync_thread = Thread(
            name='volcengine-mltask-sync-' + self._hash_tag,
            target=_sync,
            daemon=True
        )
        _sync_thread.start()

    @contextmanager
    def synchronize(self, timeout: int = 60):
        thread_name = current_thread().name
        self.log(f'{thread_name} is tring to acquire lock', 'DEBUG')
        if self._atomic_lock.acquire(timeout=timeout):
            self.log(f'{thread_name} acquired the lock', 'DEBUG')
            yield
            self._atomic_lock.release()
            self.log(f'{thread_name} released the lock', 'DEBUG')
        else:
            raise RuntimeError(f'{thread_name} could not acquire the lock')

    def _aqcuire_real_submited_lock(self, task_id: str) -> bool:
        """
        Return when the real submited tasks number is under the limit of max_real_submited_tasks
        or the task is cancelled.
        """
        if len(self._task_id_map) < self._max_real_submited_tasks:
            return True
        task_status_list = self._task_list
        submited_real_tasks = task_status_list[
            task_status_list.index.isin(self._task_id_map.keys())]
        submited_real_tasks_not_exit = submited_real_tasks[
            submited_real_tasks['status'].isin([TaskStatus.PENDING, TaskStatus.RUNNING])]
        current_count = len(submited_real_tasks_not_exit)
        return current_count < self._max_real_submited_tasks

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
    
    def _wrap_entrypoint(self, entrypoint_path: str):
        """
        Wrap the script to tos.

        Parameters
        ----------
        entrypoint_path: str
            The path of the script.
            If not exists, return the original path.
            (regard as command)
        Returns
        ----------
        dict
            Wrappered config
        """
        entrypoint_path_obj = Path(entrypoint_path)
        if not entrypoint_path_obj.is_file():
            return {
                "entrypoint_path": entrypoint_path
            }
        stored_key = sha256sum(entrypoint_path_obj)
        tos_bucket = f'tos://{self._bucket_name}'
        tos_dir = 'scripts'
        manifest = {
            "Version":"1.0",
            "RemoteRootPath":tos_bucket + '/' + tos_dir,
            "LocalRootPath":".",
            "AuthorID":"",
            "MetaInfos":[
                {
                    "Path":entrypoint_path_obj.name,
                    "StoredKey":stored_key,
                    "Size":entrypoint_path_obj.stat().st_size,
                    "PermissionMask":484,
                    "IsDir":False,
                    "SoftLink":False,
                    "LinkPath":""
                }]}
        manifest_content_str = json.dumps(manifest)
        manifest_snapshot_id = sha256sum_str(manifest_content_str)
        manifest_tos_key = f'{tos_dir}/manifest/{manifest_snapshot_id}.manifest'
        self._tos_client.put_object_from_file(
            self._bucket_name,
            f'{tos_dir}/{stored_key}',
            entrypoint_path
        )
        self._tos_client.put_object(
            self._bucket_name,
            manifest_tos_key,
            content=StringIO(manifest_content_str)
        )
        return {
            "entrypoint_path": '/tmp/code/' + entrypoint_path_obj.name,
             "tos_code_path": tos_bucket + '/' + manifest_tos_key,
            'local_code_path': '/tmp/code'
        }

    def _async_submit(
            self, 
            task_id: str,
            name: str, entrypoint_path: str, dependencies: dict = None, **config):
        # exception must be manunal catched, otherwise the exception only raised when get the future results!!!
        try:
            self.log(f'Task {name} [{task_id}] added to queue', level='DEBUG')
            should_submit = True
            if dependencies is not None and len(dependencies) > 0:
                for condition, dep_task_ids in dependencies.items():
                    cond_map = self._dependency_status_map.get(condition)
                    if isinstance(dep_task_ids, str):
                        dep_task_ids = [dep_task_ids]
                    if not isinstance(dep_task_ids, Iterable):
                        raise ValueError(
                            f"dependencies should be an iterable or str, but got {type(dep_task_ids)}")
                    should_wait = False
                    if task_id not in self._pendding_tasks:
                        should_submit = False
                    else: 
                        statuses = [self._task_list['status'][dep_task_id] for dep_task_id in dep_task_ids]
                        if all(status in cond_map['start'] for status in statuses):
                            should_submit = True
                        elif any(status in cond_map['stop'] for status in statuses):
                            should_submit = False
                        else:
                            should_wait = True
                    if should_submit:
                        if should_wait:
                            self.log(
                                f'Task {dep_task_ids} are at {statuses}, waiting for {condition} to submit task {task_id}',
                                level='DEBUG')
                            time.sleep(5)
                            self._submit_executor.submit(
                                self._async_submit, task_id, name, entrypoint_path, dependencies, **config
                            )
                            return
                    else:
                        break
            
            # wait if real submited tasks is over the limit
            self.log(f'Checking max-limitation for {task_id}', level='DEBUG')
            if not self._aqcuire_real_submited_lock(task_id):
                self.log(
                    f'Current real submited tasks is over the limit {self._max_real_submited_tasks}, waiting for some tasks to finish',
                    level='DEBUG')
                time.sleep(5)
                self._submit_executor.submit(
                    self._async_submit, task_id, name, entrypoint_path, dependencies, **config
                )
                return

            self.log(f'Checking should submit for {task_id}', level='DEBUG')
            # make sure atomicity
            with self.synchronize():
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
                _config.update(self._wrap_entrypoint(entrypoint_path))
                status = self._task_client.create_custom_task(
                    name='_'.join((self._hash_tag, name, task_id)),
                    **_config
                )
                real_task_id = status['Result']['Id']
                self._task_id_map[task_id] = real_task_id
                self.log(f'Submit {task_id} really with real id {real_task_id}', level='DEBUG')
        except Exception as e:
            if isinstance(e, RuntimeError) and str(e).startswith('cannot schedule new futures after'):
                return
            self._submit_error_tasks.add(task_id)
            self.log(
                f'Failed to submit task {task_id}: {e.__class__.__name__} [{e}]',
                level='ERROR', exc_info=True)

    def submit(self, name: str, entrypoint_path:str, dependencies: dict = None, **config) -> str:
        task_id = self._unique_task_id()
        with self.synchronize():
            self._pendding_tasks.add(task_id)
            self._task_list.loc[task_id]=pd.Series({
                    "name": name,
                    "entrypoint_path": entrypoint_path,
                    "status": TaskStatus.PENDING,
                    "dependencies": json.dumps(dependencies)
                })
            # print(self._task_list)
        self._submit_executor.submit(
            self._async_submit, task_id, name, entrypoint_path, dependencies, **config)
        return task_id

    def cancel(self, task_id: str) -> bool:
        try:
            self.log(
                f'Cancel task {task_id}',
                level='DEBUG'
            )
            # make sure atomicity
            with self.synchronize():
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
                status = self._task_client.stop_custom_task(
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
        with self.synchronize():
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
            return self._task_client.get_custom_task(
                task_id=self._task_id_map[task_id]
            )['Result']

    def status(self, task_id: str) -> TaskStatus:
        task_info = self.query(task_id)
        status = task_info['State']
        return self._status_map.get(
            status, TaskStatus.UNKNOWN)
    
    def _list_custom_tasks(self, limit: int = 100) -> pd.DataFrame:
        offset = 0
        total_list = []
        real_task_ids = list(self._task_id_map.values())
        while True:
            res_list = self._task_client.list_custom_tasks(
                task_filter=self._hash_tag,
                offset=offset,
                limit=limit
            )['Result']['List']
            total_list.extend(res_list)
            if len(res_list) < limit:
                break
            time.sleep(0.1)
            offset += limit
        total_list = pd.DataFrame(total_list)
        total_list.rename(columns=case_convert.snake_case, inplace=True)
        # check if all real tasks are in the total list
        for task_id in real_task_ids:
            if task_id not in total_list['id'].values:
                raise KeyError(f'Task {task_id} not in queried list')
        return total_list

    def list(self) -> pd.DataFrame:
        # time cost if many task so not lock to avoid block submit
        status = self._list_custom_tasks()
        if len(status) != 0:
            status.set_index('id', inplace=True)
            status_dict = status['state'].to_dict()
        else:
            status_dict = dict()
        # make sure atomicity
        with self.synchronize():
            self._task_list['status'] = self._task_list.index.map(
                lambda x: self._status_map.get(
                    status_dict.get(self._task_id_map[x], 'Queue'), 
                    TaskStatus.UNKNOWN) if x in self._task_id_map else self.status(x))
            return self._task_list.copy()

    def close(self):
        if hasattr(self, '_submit_executor'):
            super().close()
            self._submit_executor.shutdown(wait=False)
