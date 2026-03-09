from agentic_tools_gem import actions  # noqa: F401
from agentic_tools_gem.bootstrap import register_into
from agentic_tools_core.registry import ToolRegistry


def register_tools(registry: ToolRegistry) -> None:
    register_into(registry)
