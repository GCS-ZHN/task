import signal
import time
import pandas as pd
import logging

from abc import ABC, abstractmethod
from enum import Enum
from typing import Tuple, List
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    CANCELLED = 5
    UNKNOWN = 6


class TaskManager(ABC):
    """
    an abstract class for task management.

    Parameters
    ----------
    config_file : Path
        The path to the configuration file.
    """
    _instances: List['TaskManager'] = []

    @abstractmethod
    def __init__(self, config_file: Path) -> None:
        raise NotImplementedError

    def __new__(cls, *args, **kwargs) -> 'TaskManager':
        self = super().__new__(cls)
        cls._instances.append(self)
        return self

    @abstractmethod
    def submit(self, name: str, entrypoint_path:str, dependencies: dict = None, **config) -> str:
        """
        Submit a task to the task manager.

        Parameters
        ----------
        name : str
            The name of the task.
        entrypoint_path : str
            The path to the entrypoint file or command.
        config : dict
            Other configurations for the task.

        Returns
        ----------
        str
            The task ID.
        """
        raise NotImplementedError

    @abstractmethod
    def cancel(task_id: str) -> bool:
        """
        Cancel a task.

        Parameters
        ----------
        task_id : str
            The task ID.

        Returns
        ----------
        bool
            True if the task is successfully cancelled.
        """
        raise NotImplementedError

    @abstractmethod
    def query(task_id: str) -> dict:
        """
        Query the details of a task.

        Parameters
        ----------
        task_id : str
            The task ID.

        Returns
        ----------
        dict
            The details of the task.
        """
        raise NotImplementedError

    @abstractmethod
    def status(task_id: str) -> TaskStatus:
        """
        Get the status of a task.

        Parameters
        ----------
        task_id : str
            The task ID.

        Returns
        ----------
        TaskStatus
            The status of the task.
        """
        raise NotImplementedError

    @abstractmethod
    def list() -> pd.DataFrame:
        """
        Get a list of tasks.

        Returns
        ----------
        pd.DataFrame
            A DataFrame containing the details of the tasks.
            Columns: task_id, name, entrypoint_path, status (TaskStatus)
        """
        raise NotImplementedError

    def wait(self, task_id: str, timeout: int = -1, waiting_status: Tuple[TaskStatus] = None) -> TaskStatus:
        """
        Wait for a task to complete.

        Parameters
        ----------
        task_id : str
            The task ID.

        timeout : int
            The maximum time to wait in seconds.

        waiting_status : Tuple[TaskStatus]
            The status to wait for.
        Returns
        """
        if waiting_status is None:
            waiting_status = (TaskStatus.PENDING, TaskStatus.RUNNING)
        start_time = time.time()
        while True:
            status = self.status(task_id)
            if 0 < timeout < time.time() - start_time:
                return status
            if status in waiting_status:
                time.sleep(1)
                continue
            return status

    def wait_all(self, timeout: int = -1) -> dict:
        """
        Wait for all tasks to complete.

        Parameters
        ----------
        timeout : int
            The maximum time to wait every task in seconds.

        Returns
        ----------
        dict
            A dictionary containing the task ID and its status.
        """
        list_of_tasks = self.list()
        task_status = {}
        for task_id in list_of_tasks.index:
            task_status[task_id] = self.wait(task_id, timeout)
        return task_status
    
    def log(self, msg: str, level: str = 'INFO'):
        """
        Log a message.

        Parameters
        ----------
        msg : str
            The message to be logged.
        level : str
            The level of the message.
        """
        level = logging._nameToLevel.get(level, logging.INFO)
        logger = logging.getLogger(__name__)
        logger.log(level, msg)

    def close(self) -> bool:
        """
        Close the task manager.
        """
        try:
            if getattr(self, '_closed', False):
                return True
            for task_id, row in self.list().iterrows():
                if row['status'] in (TaskStatus.PENDING, TaskStatus.RUNNING):
                    self.cancel(task_id)
            self._closed = True
            return True
        except:
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

_sinint_flag = False

def _sigint_handler(signum, frame):
    global _sinint_flag
    if not _sinint_flag:
        print('Press Ctrl+C again to exit.')
        _sinint_flag = True
        return

    for idx, task_manager in enumerate(TaskManager._instances):
        if idx == 0:
            task_manager.log('Try to stop unfinished task, please wait in a minutes')
        task_manager.close()
        task_manager.log(f'{task_manager} closed.')
        TaskManager._instances.pop(idx)

    raise KeyboardInterrupt

signal.signal(signal.SIGINT, _sigint_handler)