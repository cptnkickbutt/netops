import sys
import click

# Import the argparse-based command modules
from . import speed_audit as _sa
# (Skip daily_export here until it’s ready; we can add it the same way later.)

@click.group()
def cli():
    """netops command group"""


@cli.command(
    "speed-audit",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
@click.pass_context
def speed_audit(ctx: click.Context):
    """
    Run Speed Audit (delegates to argparse inside).
    Usage: netops speed-audit [args...]
    """
    # Rebuild sys.argv so argparse inside speed_audit sees only its own args
    sys.argv = ["speed-audit", *ctx.args]
    _sa.main_entry()
