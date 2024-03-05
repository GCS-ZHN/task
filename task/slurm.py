from task.base import TaskManager, TaskStatus

import pandas as pd
import warnings
try:
    import pyslurm
except ImportError:
    warnings.warn("pyslurm is not installed. SlurmTaskManager will not be available.")
    pyslurm = None
import yaml
import time
import subprocess

from pathlib import Path
from task.base import TaskStatus
from io import StringIO
from .utils import require_cmd, wrapper_command_as_script, retry


# check if slurm are available
SBATCH = require_cmd('sbatch')
SACCT = require_cmd('sacct')
SCANCEL = require_cmd('scancel')
SQUEUE = require_cmd('squeue')

class SlurmTaskManagerBase(TaskManager):
    _status_map = {
        'RUNNING': TaskStatus.RUNNING,
        'COMPLETING': TaskStatus.RUNNING,
        'REQUEUE': TaskStatus.PENDING,
        'PENDING': TaskStatus.PENDING,
        'COMPLETED': TaskStatus.COMPLETED,
        'FAILED': TaskStatus.FAILED,
        'CANCELLED': TaskStatus.CANCELLED,
        'TIMEOUT': TaskStatus.FAILED,
        'SUSPENDED': TaskStatus.PENDING,
        'OOM': TaskStatus.FAILED,
        'PREEMPTED': TaskStatus.FAILED,
        'NODE_FAIL': TaskStatus.FAILED,
    }

    def __init__(self, config_file: Path):
        self._task_list = []
        self._config = yaml.safe_load(open(config_file))


class PySlurmTaskManager(SlurmTaskManagerBase):
    """
    A SlurmTaskManager implemented with pyslurm.
    """
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
        flag = 5
        while flag > 0:
            try:
                self.status(task_id)
                break
            except pyslurm.core.error.RPCError as e:
                # job maybe not synced to the database yet
                if str(e).startswith(f'Job {task_id} does not exist'):
                    time.sleep(1)
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


class CliSlurmTaskManager(SlurmTaskManagerBase):
    """
    A SlurmTaskManager implemented with command line interface.
    such as sbatch, squeue, scancel, sacct, etc.
    """

    def _dict_to_slurm_options(self, cmd: str, **options) -> list:
        slurm_options = [cmd]
        for k, v in options.items():
            is_bool = isinstance(v, bool)
            if is_bool and not v:
                continue
            if len(k) == 1:
                slurm_options.append(f'-{k}')
            else:
                k = k.replace('_', '-')
                slurm_options.append(f'--{k}')
            if not is_bool:
                slurm_options.append(str(v))

        return slurm_options

    def _dict_to_dependency(self, dependencies: dict) -> str:
        if dependencies is None:
            return ''
        contitons = []
        for k, v in dependencies.items():
            if isinstance(v, (list, tuple)):
                v = ':'.join(v)
            if not v:
                continue
            contitons.append(f'{k}:{v}')
        return ','.join(contitons)

    def _subprocess_run(self, cmd: list) -> subprocess.CompletedProcess:
        self.log(' '.join(cmd), level='DEBUG')
        return subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True)


    def submit(self, name: str, entrypoint_path: str, dependencies: dict = None, **config) -> str:
        _config = self._config.copy()
        _config.update(config)
        _config['job-name'] = name
        _config['kill-on-invalid-dep'] = 'yes'
        _config['parsable'] = True
        dependencies = self._dict_to_dependency(dependencies)
        if dependencies:
            _config['dependency'] = dependencies
        sbatch_cmd = self._dict_to_slurm_options(SBATCH, **_config)
        sbatch_cmd.append(str(wrapper_command_as_script(entrypoint_path)))
        proc = self._subprocess_run(sbatch_cmd)
        task_id = proc.stdout.decode().strip().split(',')[0].strip()
        int(task_id)  # check if task_id is a number

        self._task_list.append({
            'task_id': task_id,
            'name': name,
            'entrypoint_path': entrypoint_path,
        })
        return task_id

    def status(self, task_id: str) -> TaskStatus:
        job_info = self.query(task_id)
        self.log(job_info['State'], level='DEBUG')
        with open('slurm_status.log', 'a') as f:
            f.write(f'{task_id}: {job_info["State"]}\n')
        return self._status_map.get(job_info['State'], TaskStatus.UNKNOWN)
    
    def _list(self, *task_ids) -> pd.DataFrame:
        sacct_options = {
            'parsable2': True,
            'format': 'JobID,State',
            'allocations': True
        }
        squeue_options = {
            'Format': 'JobID,State',
        }
        if task_ids:
            sacct_options['jobs'] = ','.join(task_ids)
            squeue_options['job'] = sacct_options['jobs']
        sacct_cmd = self._dict_to_slurm_options(
            SACCT, 
            **sacct_options
            )
        squeue_cmd = self._dict_to_slurm_options(
            SQUEUE, 
            **squeue_options
            )
        sacct_proc = self._subprocess_run(sacct_cmd)
        squeue_proc = self._subprocess_run(squeue_cmd)
        # slurmdb is not always up to date, so we need to merge sacct and squeue
        sacct_df = pd.read_csv(
            StringIO(sacct_proc.stdout.decode()), 
            sep='|',
            header=0,
            index_col=0)
        squeue_df = pd.read_csv(
            StringIO(squeue_proc.stdout.decode()), 
            sep=r'\s+',
            header=0,
            index_col=0)
        squeue_df.rename({
            'STATE': 'State', 'JOBID': 'JobID'}, 
            axis=1, inplace=True)
        sacct_df['State'] = sacct_df['State'].str.split().str[0]
        squeue_df['State'] = squeue_df['State'].str.split().str[0]
        sacct_df.index = sacct_df.index.astype(str)
        squeue_df.index = squeue_df.index.astype(str)
        df = pd.concat([sacct_df, squeue_df])
        df = df[~df.index.duplicated(keep='last')]
        return df.copy()

    def list(self) -> pd.DataFrame:
        task_list = pd.DataFrame(
            self._task_list)
        task_list.set_index('task_id', inplace=True)
        task_details = self._list(*task_list.index)
        task_list = pd.merge(
            task_list, task_details, 
            left_index=True, right_index=True)
        task_list['status'] = task_list['State'].map(self._status_map)
        task_list = task_list[['name', 'entrypoint_path', 'status']]
        return task_list.copy()

    def cancel(self, task_id: str) -> bool:
        try:
            scancel_cmd = self._dict_to_slurm_options(SCANCEL)
            scancel_cmd.append(task_id)
            proc = self._subprocess_run(scancel_cmd)
            return True
        except Exception as e:
            self.log(e, level='ERROR')
            return False

    def query(self, task_id: str) -> dict:
        df = self._list(task_id)
        return df.loc[task_id].to_dict()


if pyslurm is not None:
    SlurmTaskManager = PySlurmTaskManager
else:
    SlurmTaskManager = CliSlurmTaskManager