---
name: skills/mermaiddiagram-kainoa
description: When generating Mermaid.js diagrams, follow these rules to avoid syntax errors and ensure compatibility:
---
1. Each node must be defined on its own line. Do not define multiple nodes on a single line.
2. Node IDs must be unique and use only letters and numbers.
3. Node labels should not contain double quotes or parentheses. If a label must include parentheses or special characters, wrap the entire label in double quotes (e.g., A[“Label with (parentheses)”]). Do not use unquoted parentheses in labels.
4. Do not use Markdown, HTML tags, or line breaks such as <br> or \n inside node labels. Use commas or semicolons to separate items in a label.
5. Chained arrows (like A –> B –> C) are not supported. Write each connection on its own line (A –> B, B –> C).
6. Comments using %% must be on their own lines, not at the end of code lines.
7. Only apply style to nodes that are already defined.
8. All connections must reference defined node IDs.
9. Avoid spaces in subgraph names, or use underscores or quotes.
10. If a node label contains parentheses, math expressions, or special characters, always wrap the label in double quotes.
11. Test the diagram in a Mermaid live editor if possible to confirm it renders without errors.​