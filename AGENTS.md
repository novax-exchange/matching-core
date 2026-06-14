# Project Codex Instructions

## Learning Workflow

- Treat this repository as the NovaX matching engine learning project.
- Work in learning mode by default: the user writes the code, and Codex explains, reviews, and gives the next small task.
- After each completed learning phase, run the relevant tests, create a Git commit, and push the commit to GitHub.

## Git And Commit Style

- Commit messages must be written in English.
- Use concise Conventional Commit style when possible, such as `feat(core): add symbol runtime batch processing`.
- Do not include Chinese text in commit messages.
- Before committing or pushing, run the relevant verification command, usually `cargo test` or `cargo test -p matching-core`.
