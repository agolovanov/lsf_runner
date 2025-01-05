import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd


class Job:
    def __init__(self, job_id):
        self.id = job_id

    def peek(self):
        print(subprocess.check_output(['bpeek', str(self.id)]).decode())

    def check_status(self) -> str:
        """Checks the current status of the job

        Returns
        -------
        str
            Status, one of the following: PEND, RUN, DONE, EXIT, SSUSP
        """
        try:
            job_status = subprocess.check_output(
                ['bjobs', '-o', 'all', '-json', str(self.id)], stderr=subprocess.DEVNULL
            )
            job_status_json = json.loads(job_status.decode())
            return job_status_json['RECORDS'][0]['STAT']
        except subprocess.CalledProcessError as e:
            return e.stdout
        except Exception as e:
            return f'Exception: {e}'

    def wait_complete(self, check_period=10, exit_wait_period=30):
        """Waits until the job is completed (by achieving either DONE or EXIT status)

        Parameters
        ----------
        check_period : float
            The time interval (in s) for checking if the job is completed, by default 10
        exit_wait_period : float
            The time interval (in s) for checking if the job with the EXIT status wasn't rerun, by default 30

        Returns
        -------
        str
            the exit status of the job (DONE or EXIT)
        """
        import time

        time.sleep(check_period)

        while (st := self.check_status()) != 'DONE' and st != 'EXIT':
            if st != 'PEND' and st != 'RUN':
                print(f'{self} status {st}')
            time.sleep(check_period)

        if st == 'EXIT':
            time.sleep(exit_wait_period)
            st = self.check_status()
            if st != 'EXIT':
                print(f'Job {self} appears to have restarted')
                return self.wait_complete()

        return st

    def __str__(self) -> str:
        return f'Job #{self.id}'

    def __repr__(self) -> str:
        return str(self)


def bool_to_str(b):
    return 'yes' if b else 'no'


@dataclass
class GpuParameters:
    number: int = 1
    mode: str = None
    job_exclusive: bool = True
    memory_required: str = None
    model: str = None

    def __str__(self) -> str:
        parameter_list = []
        parameter_list.append(f'num={self.number}')
        if self.mode is not None:
            parameter_list.append(f':mode={self.mode}')
        parameter_list.append(f':j_exclusive={bool_to_str(self.job_exclusive)}')
        if self.model is not None:
            parameter_list.append(f':gmodel={self.model}')
        if self.memory_required is not None:
            parameter_list.append(f':gmem={str(self.memory_required)}')

        return ''.join(['"'] + parameter_list + ['"'])


def span_parameters(hosts):
    return f'hosts={hosts}'


def resource_usage(memory: str = None):
    return f'mem={memory}'


def resource_requirements(
    *, select: str = None, span: str = None, resource_usage: str = None, affinity: str = None
) -> str:
    """Parameters specifying the resource requirements for the host.

    See: https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=o-r

    Parameters
    ----------
    select : str, optional
        the select string, by default None
    span : str, optional
        the span parameters string, by default None

        Use the `span_parameters` function for assistance
    resource_usage : str, optional
        resource usage string, by default None

        Use the `resource_usage` function for assistance
    affinity : str, optional
        the affinity string, by default None

    Returns
    -------
    str
        the full string for the resource requirements
    """
    parameter_list = []
    if select is not None:
        parameter_list.append(f'select[{select}]')
    if span is not None:
        parameter_list.append(f'span[{span}]')
    if resource_usage is not None:
        parameter_list.append(f'rusage[{resource_usage}]')
    if affinity is not None:
        parameter_list.append(f'affinity[{affinity}]')

    return ' '.join(parameter_list)


def output_file_string(job_name, log_folder='logs'):
    """

    Parameters
    ----------
    job_name : str
        the name of the job
    log_folder : str, optional
        the folder to store logs, by default 'logs'

    Returns
    -------
    str
        output file string (passed to -o)
    """
    if not os.path.exists(log_folder):
        print(f'Creating log folder [{log_folder}]')
        os.makedirs(log_folder)

    return os.path.join(log_folder, f'{job_name.replace("/", "_")}-%J.out')


def retrieve_bsub_job_id(s: str):
    """

    Parameters
    ----------
    s : str
        string for bsub submission

    Returns
    -------
    int
        JOB ID corresponding to the BSUB submission

    Raises
    ------
    ValueError
        If the string is incorrect
    """
    import re

    match = re.search(r'<(\d+)>', s)

    if match:
        return int(match.group(1))
    else:
        raise ValueError(f'No job ID found in string [{s}]')


def __run_bsub_command(bsub_command, ensure_completion=False) -> Job:
    print(f'Running: {" ".join(bsub_command)}', flush=True)
    job_id = retrieve_bsub_job_id(subprocess.check_output(bsub_command, stderr=subprocess.DEVNULL).decode())
    job = Job(job_id)
    print(f'Submitted {job}')

    if ensure_completion:
        st = job.wait_complete()

        if st == 'EXIT':
            print(f'{job} received an EXIT status, rerunning', flush=True)
            return __run_bsub_command(bsub_command, True)

        print(f'{job} successfully finished', flush=True)

    return Job(job_id)


def run_job(
    command,
    tasks_number: int = 1,
    job_name: str = 'job',
    queue: str = None,
    *,
    use_gpu: bool = False,
    gpu_parameters: GpuParameters = None,
    resource_requrements: str = None,
    hosts: str = None,
    rerunnable: bool = False,
    output_file: str | Path = None,
    ensure_completion: bool = False,
) -> Job:
    """Submits an LSF job

    Parameters
    ----------
    command : str
        the command to run
    tasks_number : int
        number of tasks in a job, by default 1
    job_name : str, optional
        job name, by default 'job'
    queue : str, optional
        job queue to submit to, by default None
    use_gpu : bool, optional
        request GPU, by default False
    gpu_parameters: lsf_runner.GpuParameters
        parameters to use with the GPU
    resource_requrements: str
        resourse requirements of the job

        Use the `resource_requirements` to generate.
    hosts: str, optional
        hosts to run the job on
    rerunnable : bool, optional
        make the program rerunnable or non-rerunnable (-rn flag), by default False
    output_file : str, optional
        the name of the file to forward the output to (-o flag)
    ensure_completion: bool, optional
        whether wait for the completion of the job and restart if its status is not DONE, by default False

    Returns
    -------
    lsf_runner.Job
        The Job class object corresponding to the submitted job.
    """
    if output_file is None:
        output_file = output_file_string(job_name)

    bsub_arguments = ['-J', job_name, '-o', str(output_file), '-n', str(tasks_number)]
    if queue is not None:
        bsub_arguments += ['-q', queue]

    if resource_requrements is not None:
        bsub_arguments += ['-R', resource_requrements]

    if use_gpu:
        if gpu_parameters is not None:
            gpu_parameter_string = str(gpu_parameters)
        else:
            gpu_parameter_string = '-'
        bsub_arguments += ['-gpu', gpu_parameter_string]

    if hosts is not None:
        bsub_arguments += ['-m', hosts]

    if not rerunnable:
        bsub_arguments += ['-rn']

    lsf_command = ['bsub'] + bsub_arguments + [command]
    return __run_bsub_command(lsf_command, ensure_completion)


def bhosts() -> pd.DataFrame:
    """Requests the list of hosts available to LSF with additional information through bhosts.

    Returns
    -------
    pd.DataFrame
        the resulting dataframe with the information
    """
    output = subprocess.check_output(['bhosts', '-json', '-o', 'all'])
    records = json.loads(output.decode())['RECORDS']
    return pd.DataFrame.from_records(records, index='HOST_NAME')


def lshosts() -> pd.DataFrame:
    """Requests the list of hosts available to LSF with additional information through lshosts.

    Returns
    -------
    pd.DataFrame
        _description_
    """
    output = subprocess.check_output(
        [
            'lshosts',
            '-json',
            '-o',
            'hname type model cpuf ncpus maxmem maxswp server res ndisks maxtmp rexpri nprocs ncores nthreads runwin',
        ]
    )
    data = json.loads(output.decode())['RECORDS']
    return pd.DataFrame.from_records(data, index='HOST_NAME')


def get_hosts() -> List[str]:
    """Requests the list of hosts available to LSF.

    Returns
    -------
    list
        the resulting list of hosts
    """
    hosts = bhosts()
    return hosts.index.tolist()


def lsrun(host, command, timeout=5, pass_exceptions=False):
    try:
        output = subprocess.check_output(
            f'lsrun -m {host} {command}', shell=True, timeout=timeout, stderr=subprocess.STDOUT
        )
        return output.decode().strip()
    except subprocess.CalledProcessError as e:
        if pass_exceptions:
            raise e
        else:
            return e.output.decode().strip()
    except subprocess.TimeoutExpired as e:
        if pass_exceptions:
            raise e
        else:
            return f'{e}'


def get_compatible_mpirun(allocation_debug=False):
    command = 'mpirun --mca plm_rsh_agent "blaunch" --hostfile $LSB_DJOB_HOSTFILE'
    if allocation_debug:
        command += ' --display-allocation --display-map --report-bindings'
    return command
