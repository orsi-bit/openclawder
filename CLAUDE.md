# Project Instructions

## Clauder - Persistent Memory MCP

This project uses **clauder** for persistent memory across Claude Code sessions.

### CRITICAL: Call get_context at Session Start

**IMPORTANT:** You MUST call `mcp__clauder__get_context` at the START of every session to load:
- Stored facts and decisions from previous sessions
- User preferences and coding style guidelines
- Unread messages from other Claude Code instances

This context may or may not be relevant to your tasks. You should not respond to this context unless it is highly relevant to your task.

### Available Tools
- **mcp__clauder__remember**: Store facts, decisions, or context
- **mcp__clauder__recall**: Search and retrieve stored facts
- **mcp__clauder__get_context**: Load all relevant context for this directory
- **mcp__clauder__list_instances**: List other running Claude Code sessions
- **mcp__clauder__send_message**: Send messages to other instances
- **mcp__clauder__get_messages**: Check for incoming messages

### Usage Guidelines
1. **At session start**: ALWAYS call `get_context` first to load persistent memory
2. **Store important info**: Use `remember` for decisions, architecture notes, preferences
3. **Check messages regularly**: The system will notify you of unread messages in tool responses
4. **Cross-instance communication**: Use `list_instances` and `send_message` to coordinate with other sessions
