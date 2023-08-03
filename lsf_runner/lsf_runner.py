from dataclasses import dataclass


@dataclass
class GpuParameters:
    number: int = 1
    job_exclusive: bool = True
    memory_required: str = None

    def __str__(self) -> str:
        def bool_to_str(b):
            return "yes" if b else "no"

        parameter_list = []
        parameter_list.append(f'num={self.number}')
        parameter_list.append(f':j_exclusive={bool_to_str(self.job_exclusive)}')
        if self.memory_required is not None:
            parameter_list.append(f':gmem={str(self.memory_required)}')

        return ''.join(['"'] + parameter_list + ['"'])


def run_job(command, job_number, job_name=None, queue=None, *, use_gpu=False, gpu_parameters: GpuParameters = None,
            rerunnable=False):
    """Run an LSF job

    Parameters
    ----------
    command : str
        the command to run
    job_number : int
        number of jobs
    job_name : str, optional
        job name, by default None
    queue : str, optional
        job queue to submit to, by default None
    use_gpu : bool, optional
        request GPU, by default False
    gpu_parameters: lsf_runner.GpuParameters
        parameters to use with the GPU
    rerunnable : bool, optional
        make the program rerunnable or non-rerunnable (-rn flag), by default False
    """
    import subprocess

    if job_name is None:
        job_name = 'job'

    bsub_arguments = ['-J', job_name, '-o', f'logs/{job_name}-%J.out', '-n', str(job_number)]
    if queue is not None:
        bsub_arguments += ['-q', queue]
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
