from .test_base import Base

from task.slurm import PySlurmTaskManager, CliSlurmTaskManager

class TestPySlurm(Base):
    task_manager = PySlurmTaskManager('config/pyslurm_task.yaml')

class TestCliSlurm(Base):
    task_manager = CliSlurmTaskManager('config/clislurm_task.yaml')
