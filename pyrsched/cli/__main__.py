import sys
import click
from dataclasses import dataclass

from rich.console import Console
from rich.table import Table
from rich.text import Text
from pyrsched.rpc import RPCScheduler
from halo import Halo

PYRSCHED_LOGO = "[italic bold][#e20074]P[/#e20074][white]S[/white][/bold italic]"


@dataclass
class ContextWrapper:
    scheduler: RPCScheduler
    con: Console
    json_output: bool


def make_job_table(job_list):
    table = Table(title=f"{PYRSCHED_LOGO} Job list ({len(job_list)} total)")
    table.add_column("ID", no_wrap=True)
    table.add_column("name", style="bold #e20074")
    table.add_column("interval", justify="right")
    table.add_column("next run at")
    table.add_column("is running?")

    for job in job_list:
        table.add_row(
            job["id"],
            job["name"],
            str(job["trigger"]["interval"]),
            job["next_run_time"],
            "[bold green]✔[/bold green]" if job["next_run_time"] is not None else "[bold red]х[/bold red]",
        )

    return table


@click.group()
@click.option("--json", is_flag=True, help="Output json instead of formatted text.")
@click.pass_context
def cli(ctx, json):
    # workaround for too wide calculated console width
    c = Console()
    c = Console(width=c.width-1)
    ctx.obj = ContextWrapper(scheduler=RPCScheduler(), con=c, json_output=json,)
    try:
        ctx.obj.scheduler.connect()
    except ConnectionError:
        ctx.obj.con.print(
            "[bold][red]Could not connect to server, is it running? Exiting...[/red][/bold]"
        )
        ctx.exit()


@cli.command(name="add")
@click.argument("pipeline_filename", type=click.STRING)
@click.argument("interval", type=click.INT)
@click.option("--start", is_flag=True, help="Start the job immediately after it was added to the jobstore")
@click.pass_context
def add_job_command(ctx, pipeline_filename, interval, start):
    """ Add a job to the jobstore with a given interval. This does not start the job. 
    
    \b
    PIPELINE_FILENAME: Pipeline name without suffix (.yaml)
    INTERVAL:          Execution interval in seconds (integer)
    """
    with Halo(text="Adding job...", spinner="dots", color="magenta") as spinner:
        job_id = ctx.obj.scheduler.add_job(pipeline_filename, interval)
        spinner.color = "green"
        if ctx.obj.json_output:
            spinner.stop()
            ctx.obj.con.print({"id": job_id})
        else:
            spinner.stop()
            ctx.obj.con.print(job_id, highlight=False)

    if start:
        ctx.invoke(start_job_command, job_id=job_id)

@cli.command(name="start")
@click.argument("job_id", type=click.STRING)
@click.pass_context
def start_job_command(ctx, job_id):
    """ Start a job. 
    
    JOB_ID: ID or name of the job. Name resolution works only if the name is unambiguous.
    """
    with Halo(text="Starting job...", spinner="dots", color="magenta") as spinner:
        job = ctx.obj.scheduler.start_job(job_id)
        if job is None:
            spinner.color = "red"
            ctx.obj.con.print(
                f"[bold][red]Job {ctx.obj.scheduler.get_previous_job_id()} not found.[/red][/bold]",
                highlight=False,
            )
            spinner.stop()
            ctx.exit()
        spinner.color = "green"        
        # if ctx.obj.json_output:        
        spinner.stop()
        ctx.obj.con.print(job)
        # else:
        #     spinner.stop()
        #     ctx.obj.con.print(job)


@cli.command(name="stop")
@click.argument("job_id", type=click.STRING)
@click.pass_context
def stop_job_command(ctx, job_id):
    """ Stop a job. 
    
    JOB_ID: ID or name of the job. Name resolution works only if the name is unambiguous.
    """
    with Halo(text="Stopping job...", spinner="dots", color="magenta") as spinner:
        job = ctx.obj.scheduler.stop_job(job_id)
    if job is None:
        spinner.color = "red"
        ctx.obj.con.print(
            f"[bold][red]Job {ctx.obj.scheduler.get_previous_job_id()} not found.[/red][/bold]",
            highlight=False,
        )
        spinner.stop()
        ctx.exit()
    spinner.color = "green"        
    # if ctx.obj.json_output:        
    spinner.stop()        
    ctx.obj.con.print(job)

    # if ctx.obj.json_output:
    #     ctx.obj.con.print(job)
    # else:
    #     ctx.obj.con.print(job)


@cli.command(name="list")
@click.pass_context
def list_command(ctx):
    """ List jobs known to the jobstore. """
    with Halo(text="Loading list...", spinner="dots", color="magenta") as spinner:
        job_list = ctx.obj.scheduler.list_jobs()
        spinner.color = "green"
        if ctx.obj.json_output:
            spinner.stop()
            ctx.obj.con.print(job_list)
        else:
            job_table = make_job_table(job_list)
            spinner.stop()
            ctx.obj.con.print(job_table)


@cli.command(name="status")
@click.pass_context
def status_command(ctx):
    """ Shows some status information. """
    with Halo(text="Loading", spinner="dots", color="magenta") as spinner:
        state = ctx.obj.scheduler.state
        spinner.color = "green"
        if ctx.obj.json_output:
            spinner.stop()
            ctx.obj.con.print(state)
        else:
            job_table = make_job_table(state["job_list"])
            updown = "[green]up[/green]" if state["is_running"] else "[red]down[/red]"
            runstate = {
                0: "[red]STATE_STOPPED[/red]",
                1: "[green]STATE_RUNNING[/green]",
                2: "[yellow]STATE_PAUSED[/yellow]",
            }[state["run_state"]]
            state_text = f"Server is [bold]{updown}[/bold] with run state [bold]{runstate}[/bold]. Load: {state['cpu_load']}"
            stored_job_id = ctx.obj.scheduler.get_previous_job_id()
            last_job_id_status = Text("Job-ID used by the 'use last' token (-): '")
            last_job_id_status.append(f"{stored_job_id}", style="bold white")
            last_job_id_status.append("'")
            spinner.stop()
            ctx.obj.con.print(
                f"{PYRSCHED_LOGO}: the [bold italic #e20074]P[/bold italic #e20074]ypyr-[bold italic white]S[/bold italic white]cheduler"
            )            
            ctx.obj.con.print(state_text)
            ctx.obj.con.print(job_table)
            
            ctx.obj.con.print(last_job_id_status)

if __name__ == "__main__":
    cli(prog_name="pyrsched-cli")

