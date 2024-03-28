
import time
from task.base import TaskStatus, TaskManager

class Base(object):
    task_manager: TaskManager

    def test_submit_cmd(self):
        task_id = self.task_manager.submit(
            name="test_submit_cmd",
            entrypoint_path="nvidia-smi; sleep 5; echo notok",
        )
        assert task_id is not None
        assert self.task_manager.status(task_id) in [
            TaskStatus.PENDING, TaskStatus.RUNNING]
        assert self.task_manager.wait(task_id) == TaskStatus.COMPLETED

    def test_submit_script(self):
        task_id = self.task_manager.submit(
            name="test_submit_script",
            entrypoint_path="test/test_script.sh",
        )
        assert task_id is not None
        assert self.task_manager.status(task_id) in [
            TaskStatus.PENDING, TaskStatus.RUNNING]
        assert self.task_manager.wait(task_id) == TaskStatus.COMPLETED

    def test_cancel_task(self):
        task_id = self.task_manager.submit(
            name="test_cancel_task",
            entrypoint_path="nvidia-smi; sleep 3; echo notok",
        )
        assert task_id is not None
        time.sleep(1)
        assert self.task_manager.status(task_id) in [
            TaskStatus.PENDING, TaskStatus.RUNNING]
        assert self.task_manager.cancel(task_id)
        assert self.task_manager.wait(task_id) == TaskStatus.CANCELLED

    def test_depes_afterok_1(self):
        task_id1 = self.task_manager.submit(
            name="test_depes_afterok_1-deps",
            entrypoint_path="nvidia-smi; sleep 5; echo ok",
        )
        task_id2 = self.task_manager.submit(
            name="test_depes_afterok_1-wants",
            entrypoint_path="nvidia-smi; sleep 1; echo ok",
            dependencies={"afterok": task_id1}
        )
        assert task_id1 is not None
        assert task_id2 is not None
        assert self.task_manager.wait(
            task_id1, waiting_status=(TaskStatus.PENDING,)) in [
                TaskStatus.RUNNING, TaskStatus.COMPLETED]
        assert self.task_manager.wait(task_id2) == TaskStatus.COMPLETED


    def test_depes_afterok_2(self):
        task_id1 = self.task_manager.submit(
            name="test_depes_afterok_2-deps",
            entrypoint_path="exit 1",
        )
        task_id2 = self.task_manager.submit(
            name="test_depes_afterok_2-wants",
            entrypoint_path="nvidia-smi; sleep 1; echo ok",
            dependencies={"afterok": task_id1}
        )
        assert task_id1 is not None
        assert task_id2 is not None
        assert self.task_manager.wait(
            task_id1, waiting_status=(TaskStatus.PENDING,)) in [
                TaskStatus.RUNNING, TaskStatus.FAILED]
        assert self.task_manager.wait(task_id2) == TaskStatus.CANCELLED

    def test_linked_deps(self):
        for i in range(30):
          task_id = self.task_manager.submit(
            name="test_linked_deps-{}".format(i),
            entrypoint_path="nvidia-smi; sleep 5; echo ok",
            dependencies = {"afterok": task_id} if i > 0 else None
            )
        all_status = self.task_manager.list()['status']==TaskStatus.RUNNING
        assert all_status.sum() <= 1
        # self.task_manager.wait(task_id)

    def test_depes_afternotok_1(self):
        task_id1 = self.task_manager.submit(
            name="test_depes_afternotok_1-deps",
            entrypoint_path="exit 1",
        )
        task_id2 = self.task_manager.submit(
            name="test_depes_afternotok_1-wants",
            entrypoint_path="nvidia-smi; sleep 1; echo ok",
            dependencies={"afternotok": task_id1}
        )
        assert task_id1 is not None
        assert task_id2 is not None
        assert self.task_manager.wait(
            task_id1, waiting_status=(TaskStatus.PENDING,)) in  [
                TaskStatus.RUNNING, TaskStatus.FAILED]
        assert self.task_manager.wait(task_id2) == TaskStatus.COMPLETED


    def test_depes_afternotok_2(self):
        task_id1 = self.task_manager.submit(
            name="test_depes_afternotok_2-deps",
            entrypoint_path="nvidia-smi; sleep 3; echo ok;",
        )
        task_id2 = self.task_manager.submit(
            name="test_depes_afternotok_2-wants",
            entrypoint_path="nvidia-smi; sleep 1; echo ok",
            dependencies={"afternotok": task_id1}
        )
        assert task_id1 is not None
        assert task_id2 is not None
        assert self.task_manager.wait(
            task_id1, waiting_status=(TaskStatus.PENDING,)) in [
                TaskStatus.RUNNING, TaskStatus.COMPLETED]
        assert self.task_manager.wait(task_id2) == TaskStatus.CANCELLED


    def test_depes_composition(self):
        afterok_task_id1 = self.task_manager.submit(
            name="test_depes_composition-deps1",
            entrypoint_path="nvidia-smi; sleep 3; echo ok",
        )
        afterok_task_id2 = self.task_manager.submit(
            name="test_depes_composition-deps2",
            entrypoint_path="nvidia-smi; sleep 3; echo ok",
        )
        afternotok_task_id1 = self.task_manager.submit(
            name="test_depes_composition-deps3",
            entrypoint_path="nvidia-smi; sleep 3; echo notok; exit 1",
        )
        final_task_id = self.task_manager.submit(
            name="test_depes_composition-wants",
            entrypoint_path="nvidia-smi; sleep 1; echo ok",
            dependencies={
                "afterok": [afterok_task_id1, afterok_task_id2],
                "afternotok": afternotok_task_id1
            }
        )
        assert final_task_id is not None
        assert self.task_manager.wait(
            final_task_id) == TaskStatus.COMPLETED
    
    def test_many_tasks(self):
        ids = []
        for i in range(200):
            task_id = self.task_manager.submit(
                name="test_many_tasks-{}".format(i),
                entrypoint_path="nvidia-smi; sleep 5; echo ok",
            )
            ids.append(task_id)
        self.task_manager.wait_all()
        task_list = self.task_manager.list()
        task_list = task_list.loc[ids]
        assert task_list['status'].isin([TaskStatus.COMPLETED]).all()
