from abc import ABC, abstractmethod
from enum import Enum
from functools import wraps
import time
import pandas as pd


class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    CANCELLED = 5
    UNKNOWN = 6


class TaskManager(ABC):

    @abstractmethod
    def submit(self, name: str, entrypoint_path:str, **config) -> str:
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

    def wait(self, task_id: str, timeout: int = -1) -> TaskStatus:
        """
        Wait for a task to complete.

        Parameters
        ----------
        task_id : str
            The task ID.

        timeout : int
            The maximum time to wait in seconds.

        Returns
        """
        start_time = time.time()
        while True:
            status = self.status(task_id)
            if 0 < timeout < time.time() - start_time:
                return status
            if status in [TaskStatus.RUNNING, TaskStatus.PENDING]:
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


def retry(func):
    """
    A decorator for retrying a function.

    Parameters
    ----------
    func : function
        The function to be retried.

    Returns
    ----------
    function
        The decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        error = None
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                time.sleep(1)
                error = e
        raise error
    return wrapper