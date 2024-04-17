from .lsf_runner import run_job, GpuParameters, ResourceRequirements, span_parameters, resource_usage


def run_fbpic(script, queue: str, tasks_number: int = 1, *, job_name: str = 'fbpic', gpu_memory=None,
              conda_environment: str = None, **kwargs):
    if tasks_number > 1:
        script = f'mpirun -n {tasks_number} python {script}'
    elif tasks_number == 1:
        script = f'python {script}'
    else:
        raise ValueError('Invalid tasks number')

    if conda_environment is not None:
        script = '; '.join([
            'source ~/miniconda3/etc/profile.d/conda.sh',
            f'conda activate {conda_environment}',
            script,
        ])

    gpu_parameters = GpuParameters(1, job_exclusive=True, memory_required=gpu_memory, mode='exclusive_process')
    resources = ResourceRequirements(span=span_parameters(hosts=1), affinity='affinity[thread*1]',
                                     resource_usage=resource_usage(memory='10G'))

    run_job(script, tasks_number, job_name, queue, use_gpu=True, gpu_parameters=gpu_parameters,
            resource_requrements=resources, **kwargs)