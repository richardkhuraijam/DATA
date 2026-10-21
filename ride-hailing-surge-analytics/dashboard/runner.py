"""Alias forwarding to dashboards.runner to support either import convention."""
from dashboards.runner import *
from dashboards.runner import cli

if __name__ == "__main__":
    cli()
