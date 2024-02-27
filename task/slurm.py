from task.base import TaskManager, TaskStatus, retry

import pandas as pd
import warnings
try:
    import pyslurm
except ImportError:
    warnings.warn("pyslurm is not installed. SlurmTaskManager will not be available.")
    pyslurm = None
import yaml
import time
import tempfile

from pathlib import Path
from task.base import TaskStatus


def wrapper_command_as_script(command: str) -> Path:
    if Path(command).exists():
        return Path(command)
    script = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)
    script.write('#!/usr/bin/env bash\n'.encode())
    script.write(command.encode())
    script.close()
    return Path(script.name)


class SlurmTaskManager(TaskManager):

    _status_map = {
        'RUNNING': TaskStatus.RUNNING,
        'PENDING': TaskStatus.PENDING,
        'COMPLETED': TaskStatus.COMPLETED,
        'FAILED': TaskStatus.FAILED,
        'CANCELLED': TaskStatus.CANCELLED
    }

    def __init__(self, config_file: Path):
        if pyslurm is None:
            raise ImportError("pyslurm is not installed.")
        self._task_list = []
        self._config = yaml.safe_load(open(config_file))

    def submit(self, name: str, entrypoint_path: str, dependencies: dict = None, **config) -> str:
        _config = self._config.copy()
        _config.update(config)
        desc = pyslurm.JobSubmitDescription(
            name=name,
            script=wrapper_command_as_script(entrypoint_path),
            dependencies=dependencies,
            # kill options to avoid hanging jobs in the queue
            kill_on_invalid_dependency=True,
            kill_on_node_fail=True,
            **_config
        )
        desc.load_sbatch_options()
        task_id = desc.submit()
        task_id = str(task_id)
        flag = 3
        while flag > 0:
            try:
                self.status(task_id)
                break
            except pyslurm.core.error.RPCError as e:
                # job maybe not synced to the database yet
                if str(e).startswith(f'Job {task_id} does not exist'):
                    time.sleep(0.5)
                    flag -= 1
                    if flag == 0:
                        raise e
                    continue
                raise e

        self._task_list.append({
            'task_id': task_id,
            'name': name,
            'entrypoint_path': entrypoint_path,
        })
        return task_id

    @retry
    def status(self, task_id: str) -> TaskStatus:
        state = pyslurm.db.Job.load(int(task_id)).state
        return self._status_map.get(state, TaskStatus.UNKNOWN)

    def list(self) -> pd.DataFrame:
        task_list = pd.DataFrame(
            self._task_list,
            columns=['task_id', 'name', 'entrypoint_path'])
        task_list.set_index('task_id', inplace=True)
        task_list['status'] = task_list.index.map(self.status)
        return task_list

    def cancel(self, task_id: str) -> bool:
        try:
            pyslurm.Job(int(task_id)).cancel()
            return True
        except Exception as e:
            self.log(e, level='ERROR')
            return False

    def query(self, task_id: str) -> dict:
        job = pyslurm.db.Job.load(int(task_id))
        return job.as_dict()
