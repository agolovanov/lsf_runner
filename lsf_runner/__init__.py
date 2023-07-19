import subprocess

def run_job(command, job_number, job_name=None, queue=None, use_gpu=False, rerunnable=False):
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
    rerunnable : bool, optional
        make the program rerunnable or non-rerunnable (-rn flag), by default False
    """
    if job_name is None:
        job_name = 'job'
    
    bsub_arguments = ['-J', job_name, '-o', f'logs/{job_name}-%J.out', '-n', str(job_number)]
    if queue is not None:
        bsub_arguments += ['-q', queue]
    if use_gpu:
        bsub_arguments += ['-gpu', '-']
    if not rerunnable:
        bsub_arguments += ['-rn']
    

    lsf_command = ['bsub'] + bsub_arguments + [command] 
    print(f'Running: {" ".join(lsf_command)}')
    subprocess.run(lsf_command)