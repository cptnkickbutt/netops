# src/netops/cli/__init__.py
from __future__ import annotations
import click

from .speed_audit import speed_audit_cli      # <— Click command now
from .daily_export import daily_export_cli    # already Click
from .mass_config import mass_config_cli
from .pw_gen import pw_gen_cli

@click.group()
def cli():
    """NetOps CLI entrypoint — run network automation tools."""
    pass

cli.add_command(speed_audit_cli, name="speed-audit")
cli.add_command(daily_export_cli, name="daily-export")
cli.add_command(mass_config_cli, name="mass-config")
cli.add_command(pw_gen_cli, name="pw-gen")

if __name__ == "__main__":
    cli()
