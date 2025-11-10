# src/netops/cli/__init__.py
from __future__ import annotations
import click

@click.group()
def cli():
    """NetOps CLI entrypoint — run network automation tools."""
    pass


# -----------------------------
# speed-audit command (legacy-friendly)
# -----------------------------
# Your repo originally invoked: _sa.main_entry()
# Keep that behavior so we don't break anything.
@cli.command("speed-audit")
@click.pass_context
def _speed_audit_cmd(ctx: click.Context):
    """Run the Speed Audit workflow."""
    try:
        from . import speed_audit as _sa
    except Exception as e:
        raise click.ClickException(f"Unable to import speed_audit: {e}")

    # Prefer main_entry() if present; else try main(); else error clearly.
    if hasattr(_sa, "main_entry"):
        _sa.main_entry()
        return
    if hasattr(_sa, "main"):
        _sa.main()  # type: ignore[call-arg]
        return
    raise click.ClickException(
        "speed_audit module found but exposes neither main_entry() nor main()."
    )


# -----------------------------
# daily-export command (flexible)
# -----------------------------
# If daily_export.py defines a Click command (daily_export_cli), register it.
# Otherwise, provide a wrapper that calls main()/main_entry() for parity.
try:
    from .daily_export import daily_export_cli as _daily_export_cli  # preferred
    cli.add_command(_daily_export_cli, name="daily-export")
except Exception:
    @cli.command("daily-export")
    @click.option("-i", "--inventory", default="propertyinformation.csv",
                  help="CSV: Property,IP,UserEnv,PwEnv")
    @click.option("-s", "--single", is_flag=True, help="Interactively select properties.")
    @click.option("--no-email", is_flag=True, help="Email only Eric instead of full distro.")
    @click.option("--keep", is_flag=True, help="Keep local zip (skip cleanup).")
    @click.option("--keep-remote-logs", is_flag=True,
                  help="Do NOT delete remote log.N.txt after download (debug/test).")
    @click.option("--log-file", default=None)
    @click.option("--log-level", default="INFO",
                  type=click.Choice(["DEBUG","INFO","WARNING","ERROR","CRITICAL"]))
    @click.pass_context
    def _daily_export_cmd(ctx: click.Context, **kwargs):
        """Run the Daily Exports workflow (export.rsc, logs, hotspot)."""
        try:
            from . import daily_export as _de
        except Exception as e:
            raise click.ClickException(f"Unable to import daily_export: {e}")

        # If the module exposes a Click command, call it; else try main()/main_entry().
        if hasattr(_de, "daily_export_cli"):
            # Call the function directly with parsed kwargs (acts like a regular function here)
            return _de.daily_export_cli.callback(**kwargs)  # type: ignore[attr-defined]
        if hasattr(_de, "main_entry"):
            return _de.main_entry()  # type: ignore[misc]
        if hasattr(_de, "main"):
            return _de.main()  # type: ignore[misc]
        raise click.ClickException(
            "daily_export module found but exposes neither daily_export_cli, main_entry(), nor main()."
        )


if __name__ == "__main__":
    cli()
