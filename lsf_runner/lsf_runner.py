from dataclasses import dataclass


def bool_to_str(b):
    return "yes" if b else "no"


@dataclass
class GpuParameters:
    number: int = 1
    job_exclusive: bool = True
    memory_required: str = None
    model: str = None

    def __str__(self) -> str:
        parameter_list = []
        parameter_list.append(f'num={self.number}')
        parameter_list.append(f':j_exclusive={bool_to_str(self.job_exclusive)}')
        if self.model is not None:
            parameter_list.append(f':gmodel={self.model}')
        if self.memory_required is not None:
            parameter_list.append(f':gmem={str(self.memory_required)}')

        return ''.join(['"'] + parameter_list + ['"'])


class SpanParameters:
    span_string: str = ''

    def __str__(self) -> str:
        return f'span[{self.span_string}]'

    def __init__(self, hosts):
        self.span_string = f'hosts={hosts}'


@dataclass
class ResourceRequirements:
    span: SpanParameters = None

    def __str__(self) -> str:
        parameter_list = []
        if self.span is not None:
            parameter_list.append(str(self.span))

        return f'"{" ".join(parameter_list)}"'


def run_job(command, tasks_number, job_name=None, queue=None, *, use_gpu=False, gpu_parameters: GpuParameters = None,
            resource_requrements: ResourceRequirements = None, rerunnable=False):
    """Run an LSF job

    Parameters
    ----------
    command : str
        the command to run
    tasks_number : int
        number of tasks in a job
    job_name : str, optional
        job name, by default None
    queue : str, optional
        job queue to submit to, by default None
    use_gpu : bool, optional
        request GPU, by default False
    gpu_parameters: lsf_runner.GpuParameters
        parameters to use with the GPU
    resource_requrements: lsf_runner.ResourceRequirements
        resourse requirements of the job
    rerunnable : bool, optional
        make the program rerunnable or non-rerunnable (-rn flag), by default False
    """
    import subprocess

    if job_name is None:
        job_name = 'job'

    bsub_arguments = ['-J', job_name, '-o', f'logs/{job_name.replace("/", "_")}-%J.out', '-n', str(tasks_number)]
    if queue is not None:
        bsub_arguments += ['-q', queue]

    if resource_requrements is not None:
        bsub_arguments += ['-R', str(resource_requrements)]

    if use_gpu:
        if gpu_parameters is not None:
            gpu_parameter_string = str(gpu_parameters)
        else:
            gpu_parameter_string = '-'
        bsub_arguments += ['-gpu', gpu_parameter_string]
    if not rerunnable:
        bsub_arguments += ['-rn']

    lsf_command = ['bsub'] + bsub_arguments + [command]
    print(f'Running: {" ".join(lsf_command)}')
    subprocess.run(lsf_command)
