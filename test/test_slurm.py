from .test_base import Base

from task.slurm import SlurmTaskManager

class TestSlurm(Base):
    task_manager = SlurmTaskManager('config/slurm_task.yaml')