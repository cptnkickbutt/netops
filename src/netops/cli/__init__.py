from __future__ import annotations

import click

from .daily_export import daily_export_cli
from .mass_config import mass_config_cli
from .pw_gen import pw_gen_cli
from .speed_audit import speed_audit_cli
from .wireless_info import wireless_info_cli


@click.group()
def cli():
    """NetOps CLI entrypoint - run network automation tools."""
    pass


cli.add_command(speed_audit_cli, name="speed-audit")
cli.add_command(daily_export_cli, name="daily-export")
cli.add_command(mass_config_cli, name="mass-config")
cli.add_command(pw_gen_cli, name="pw-gen")
cli.add_command(wireless_info_cli, name="wireless-info")


if __name__ == "__main__":
    cli()
