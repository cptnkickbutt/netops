import click

# Import your existing subcommands.
# If your current commands live in src/netops/cli/*.py, import them here.
# Example:
# from netops.cli.speed_audit import main as speed_audit
# from netops.cli.daily_export import main as daily_export

@click.group()
def cli():
    """netops command group"""

# Register commands as they’re ready:
# cli.add_command(speed_audit, name="speed-audit")
# cli.add_command(daily_export, name="daily-export")
