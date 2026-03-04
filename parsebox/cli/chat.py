"""Chat loop -- interactive conversation with the parsebox agent."""

import asyncio
import logging

from parsebox.agent.context import DatasetContext
from parsebox.agent.session import AgentSession
from parsebox.cli import display

logger = logging.getLogger(__name__)


async def chat_loop(ctx: DatasetContext) -> str | None:
    """Run the interactive chat loop for a dataset.

    Returns:
        "/quit" if user wants to exit entirely, None to go back to menu.
    """
    session = AgentSession(ctx)

    try:
        display.console.print()
        display.show_dataset_status(ctx.dataset, ctx.source_folder)
        display.show_chat_help()

        display.console.print("[muted]Starting agent session...[/muted]")
        await session.start()
        display.console.print("[success]Agent ready.[/success]")
        display.console.print()

        # If the dataset is fresh with a source folder, kick off exploration
        if ctx.dataset.status == "draft" and ctx.source_folder and not ctx.dataset.documents:
            display.console.print("[muted]Agent is exploring your files...[/muted]")
            await _agent_turn(
                session,
                f"I just connected you to a new dataset. The source folder is {ctx.source_folder}. "
                "Please explore the files, understand their structure, and propose a schema.",
            )

        while True:
            user_input = display.prompt_input("parsebox")

            if not user_input.strip():
                continue

            # Handle slash commands
            if user_input.strip().startswith("/"):
                cmd = user_input.strip().lower()
                if cmd in ("/quit", "/exit", "/q"):
                    return "/quit"
                if cmd in ("/back", "/menu"):
                    return None
                if cmd == "/status":
                    ctx.reload_dataset()
                    display.show_dataset_status(ctx.dataset, ctx.source_folder)
                    continue
                if cmd == "/schema":
                    ctx.reload_dataset()
                    if ctx.dataset.schema_:
                        schema = ctx.dataset.schema_
                        lines = [f"Schema: {schema.name}"]
                        for f in schema.fields:
                            req = " (required)" if f.required else ""
                            lines.append(f"  {f.name}: {f.type}{req} -- {f.description}")
                        display.console.print("\n".join(lines))
                    else:
                        display.console.print("[muted]No schema defined yet.[/muted]")
                    continue
                if cmd == "/help":
                    display.show_chat_help()
                    continue
                display.show_error(f"Unknown command: {cmd}. Type /help for options.")
                continue

            # Send to agent
            await _agent_turn(session, user_input)

    except KeyboardInterrupt:
        display.console.print()
        return None
    finally:
        await session.stop()


async def _agent_turn(session: AgentSession, message: str) -> None:
    """Send a message to the agent and display the response."""
    try:
        text_buffer = []
        async for event_type, content in session.send(message):
            if event_type == "text":
                text_buffer.append(content)
            elif event_type == "tool_use":
                # If we have buffered text, flush it first
                if text_buffer:
                    display.show_agent_text("".join(text_buffer))
                    text_buffer = []
                display.show_tool_use(content)
            elif event_type == "done":
                if text_buffer:
                    display.show_agent_text("".join(text_buffer))
                    text_buffer = []

        # Flush any remaining text
        if text_buffer:
            display.show_agent_text("".join(text_buffer))

    except Exception as e:
        logger.error("Agent error: %s", e, exc_info=True)
        display.show_error(f"Agent error: {e}")
