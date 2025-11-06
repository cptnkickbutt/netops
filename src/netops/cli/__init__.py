import click
from .speed_audit import main as speed_audit
from .daily_export import main as daily_export

@click.group()
def cli():
    """netops command group"""

cli.add_command(speed_audit, name="speed-audit")
cli.add_command(daily_export, name="daily-export")
