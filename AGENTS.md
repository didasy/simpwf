# AGENTS.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## 5. Tooling (mandatory)

### Skills
- Before any response or action, invoke Skill tool twice with exact names `caveman`, then `using-superpowers`. Never Read skill files.

### jcodemunch (code search first)
- Session start: call `jcodemunch_guide` once, then `resolve_repo` with the absolute folder path to confirm the index is present (`list_repos` as fallback).
- This repo index: `local/simpwf-3c83eba5`, source root `/home/didasy/project/simpwf`.
- Discovery order: `search_symbols` → `get_context_bundle` (or `get_symbol_source`) → `search_text` (literals/comments; `is_regex=true` for regex) → `find_references`/`find_importers` (usages) → `get_ranked_context` (task context within token budget) → `get_file_tree`/`get_repo_outline` (structure).
- Never use Grep/Glob for code discovery in an indexed repo. Use Read only on the exact file about to edit (Edit requires a prior Read in the same conversation).
- Re-index: after editing source files, call `index_folder` with the absolute folder path. Never call `index_repo` with a local path (GitHub URLs only). Skip re-indexing for read-only work when the index is present.
- Deferred schemas: if a jcodemunch tool call fails, load it first via ToolSearch `select:<name>`.

### GitHub
- Use `default.github___*` tools for all GitHub reads/writes. Never the `gh` CLI, curl, WebSearch, or FetchUrl for GitHub.

