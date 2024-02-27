# Task

This is distributed simple task manager on Linux, support multiple backends

- [x] Volcengine based on [official sdk](https://github.com/volcengine/ml-platform-sdk-python/)
- [x] Slurm cluster based on [pyslurm](https://github.com/PySlurm/pyslurm)
- [ ] multiprocess python library (TODO)

## Installment
Clone current repo and execute following command
```bash
bash install.sh <backend> # backend could be 'slurm', 'volcengine' or all
```
you should make sure slurm is installed before run previous
command if you want use slurm as backend.

## Configuration

Some necessary configuration is required for different backends.

1. For slurm
General config yaml example. Actually you can config all at sbatch script
```yaml
partitions: gpu
gres_per_node:
  gpu: 1
threads_per_core: 1

```

2. For volcengine
First we need generate an access key and sercet key on [VolcEngine control panel](https://console.volcengine.com/iam/keymanage/).
Set them as ENV
```bash
export VOLC_ACCESSKEY=**********
export VOLC_SECRETKEY=**************
export VOLC_REGION=cn-beijing

```
Or save them to `~/.volc/credentials`
```conf
[default]
access_key_id     = **********
secret_access_key = **************
```
Region to `~/.volc/config`
```conf
[default]
region       = cn-beijing
```
And then you can save a general task config file.
```yaml
ImageId: vemlp-cn-beijing.cr.volces.com/preset-images/slurm:0.7.0
ResourceQueueId: q-********
Framework: Custom
TaskRoleSpecs:
  - RoleName: worker
    RoleReplicas: 1
    ResourceSpecID: ml.xni3.5xlarge # GPU * 1
Storages:
  - Type: Vepfs
    MountPath: /path/you/want/to/mounted
    VepfsName: vepfs-******   # vepfs resource name
    VepfsId: vepfs-********** # vepfs resource id
    SubPath: subpath/of/vepfs # allocated by IT
    ReadOnly: true
```

## Usage 

Following example is for using Volcengine. Other backends are similar to it.

```python
from task import VolcengineMLTaskManager
# create task manager
volcengine_task_manager = VolcengineMLTaskManager('config/volc_mltask.yaml')

# submit a task to run nvidia-smi
task_id1 = volcengine_task_manager.submit(
    name='test_task_1',
    entrypoint_path='nvidia-smi'
)

# submit a task and pending until task1 finished
task_id2 = volcengine_task_manager.submit(
    'test_task_2',
    'nvidia-smi; sleep 2; echo hello',
    dependencies={
        'afterok': task_id1
    }
)

# list all submit task
volcengine_task_manager.list()

# query status of specific task, return a TaskStatus Enum Object
volcengine_task_manager.status(task_id)

# cancel a task
volcengine_task_manager.cancel(task_id)

# wait task2 finished
volcengine_task_manager.wait(task_id2)
```
