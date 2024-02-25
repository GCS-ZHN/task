from task.base import TaskManager, TaskStatus, retry

import pandas as pd
import pyslurm
import yaml
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

    def __init__(self):
        self._task_list = []
        self._config = yaml.safe_load(open('config/slurm_task.yaml'))

    def submit(self, name: str, entrypoint_path: str, **config) -> str:
        config.update(self._config)
        desc = pyslurm.JobSubmitDescription(
            name=name,
            script=wrapper_command_as_script(entrypoint_path),
            **config
        )
        desc.load_sbatch_options()
        task_id = desc.submit()
        task_id = str(task_id)
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
        return pyslurm.Job(int(task_id)).cancel()

    def query(self, task_id: str) -> dict:
        job = pyslurm.db.Job.load(int(task_id))
        return job.as_dict()
