from .test_base import Base
from task.volcengine import VolcengineMLTaskManager
class TestVolcengine(Base):
    task_manager = VolcengineMLTaskManager('config/volc_mltask.yaml')