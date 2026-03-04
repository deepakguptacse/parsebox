"""Agent session management.

Wraps ClaudeSDKClient with dataset-specific tools and system prompt.
One session per dataset -- tools are bound to that dataset's context.
"""

import logging

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    TextBlock,
    create_sdk_mcp_server,
)

from parsebox.agent.context import DatasetContext
from parsebox.agent.prompts import build_system_prompt
from parsebox.agent.tools import create_dataset_tools

logger = logging.getLogger(__name__)

SERVER_NAME = "parsebox"
SERVER_VERSION = "0.2.0"


class AgentSession:
    """Manages one ClaudeSDKClient session bound to a dataset.

    Usage:
        session = AgentSession(ctx)
        await session.start()
        async for chunk in session.send("What's in these files?"):
            print(chunk)
        await session.stop()
    """

    def __init__(self, ctx: DatasetContext):
        self.ctx = ctx
        self.client: ClaudeSDKClient | None = None
        self._tools = []

    async def start(self) -> None:
        """Create tools, MCP server, and start the client."""
        self._tools = create_dataset_tools(self.ctx)

        server = create_sdk_mcp_server(
            name=SERVER_NAME,
            version=SERVER_VERSION,
            tools=self._tools,
        )

        tool_names = [f"mcp__{SERVER_NAME}__{t.name}" for t in self._tools]
        system_prompt = build_system_prompt(self.ctx)

        options = ClaudeAgentOptions(
            system_prompt=system_prompt,
            mcp_servers={SERVER_NAME: server},
            allowed_tools=tool_names,
            model="sonnet",
            permission_mode="bypassPermissions",
        )

        self.client = ClaudeSDKClient(options=options)
        await self.client.__aenter__()
        logger.info(
            "Agent session started for dataset '%s' with %d tools",
            self.ctx.dataset.name,
            len(self._tools),
        )

    async def send(self, user_message: str):
        """Send a message and yield text chunks from the response.

        Yields:
            Tuples of (event_type, content) where event_type is one of:
            - "text": a text chunk from the assistant
            - "tool_use": a tool being invoked (name)
            - "done": response complete
        """
        if not self.client:
            raise RuntimeError("Session not started. Call start() first.")

        # Refresh system prompt to reflect any state changes
        self.ctx.reload_dataset()

        await self.client.query(user_message)

        async for msg in self.client.receive_response():
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if isinstance(block, TextBlock):
                        yield ("text", block.text)
                    elif hasattr(block, "name"):
                        # ToolUseBlock
                        yield ("tool_use", block.name)
            elif isinstance(msg, ResultMessage):
                yield ("done", "")

    async def stop(self) -> None:
        """Clean up the session."""
        if self.client:
            await self.client.__aexit__(None, None, None)
            self.client = None
        self.ctx.cleanup()
        logger.info("Agent session stopped for dataset '%s'", self.ctx.dataset.name)
