import type { ToolCall } from "@mitsuro/api";
import { diffLines, diffWordsWithSpace } from "diff";

export type ToolDiffRowKind = "addition" | "deletion" | "metadata" | "context";

export interface ToolDiffRange {
  start: number;
  end: number;
}

export interface ToolDiffRow {
  kind: ToolDiffRowKind;
  content: string;
  prefix: string;
  oldLine?: number;
  newLine?: number;
  changedRanges?: ToolDiffRange[];
}

export interface ToolDiffPeekModel {
  rows: ToolDiffRow[];
  changedRowCount: number;
}

export type ToolDiffPresentation =
  | {
      kind: "patch";
      patch: string;
      filePath?: string;
      summary?: string;
      additions: number;
      deletions: number;
    }
  | {
      kind: "files";
      oldFile: { name: string; contents: string };
      newFile: { name: string; contents: string };
      filePath?: string;
      summary?: string;
      additions: number;
      deletions: number;
    };

const DIFF_TOOL_NAMES = new Set([
  "edit",
  "write",
  "multiedit",
  "multi_edit",
  "apply_patch",
  "patch",
]);

export function isDiffTool(toolName: string): boolean {
  return DIFF_TOOL_NAMES.has(toolName.toLowerCase());
}

/** Cheap +/− counts for Live Activity / badges without building full diff models. */
export function getToolDiffStats(
  toolCall: ToolCall,
): { additions: number; deletions: number } | null {
  if (!isDiffTool(toolCall.name)) return null;

  const args = toolCall.arguments ?? {};
  const envelope = parseToolEnvelope(toolCall.output);
  const emittedPatch = envelope?.diff ?? rawUnifiedPatch(toolCall.output);
  if (emittedPatch) {
    return countPatchChanges(emittedPatch);
  }

  const normalizedName = toolCall.name.toLowerCase();
  if (normalizedName === "apply_patch" || normalizedName === "patch") {
    const patch = convertApplyPatchToUnified(firstString(args.patch));
    return patch ? countPatchChanges(patch) : null;
  }

  if (normalizedName === "edit") {
    const oldContents = firstString(args.old_string);
    const newContents = firstString(args.new_string);
    if (oldContents === undefined || newContents === undefined) return null;
    return {
      additions: lineCount(newContents),
      deletions: lineCount(oldContents),
    };
  }

  if (normalizedName === "write") {
    const contents = firstString(args.content);
    if (contents === undefined) return null;
    return { additions: lineCount(contents), deletions: 0 };
  }

  return null;
}

export function buildToolDiffPresentation(
  toolCall: ToolCall,
): ToolDiffPresentation | null {
  if (!isDiffTool(toolCall.name)) return null;

  const args = toolCall.arguments ?? {};
  const filePath = firstString(args.file_path, args.path);
  const envelope = parseToolEnvelope(toolCall.output);
  const emittedPatch = envelope?.diff ?? rawUnifiedPatch(toolCall.output);

  if (emittedPatch) {
    const stats = countPatchChanges(emittedPatch);
    return {
      kind: "patch",
      patch: emittedPatch,
      filePath: filePath ?? inferPatchPath(emittedPatch),
      summary: envelope?.summary,
      ...stats,
    };
  }

  const normalizedName = toolCall.name.toLowerCase();
  if (normalizedName === "apply_patch" || normalizedName === "patch") {
    const patch = convertApplyPatchToUnified(firstString(args.patch));
    if (!patch) return null;
    return {
      kind: "patch",
      patch,
      filePath: inferPatchPath(patch),
      summary: envelope?.summary,
      ...countPatchChanges(patch),
    };
  }

  if (normalizedName === "edit") {
    const oldContents = firstString(args.old_string);
    const newContents = firstString(args.new_string);
    if (oldContents === undefined || newContents === undefined) return null;
    const name = filePath || "edited-file";
    return {
      kind: "files",
      oldFile: { name, contents: oldContents },
      newFile: { name, contents: newContents },
      filePath,
      summary: envelope?.summary,
      additions: lineCount(newContents),
      deletions: lineCount(oldContents),
    };
  }

  if (normalizedName === "write") {
    const contents = firstString(args.content);
    if (contents === undefined) return null;
    const name = filePath || "new-file";
    return {
      kind: "files",
      oldFile: { name, contents: "" },
      newFile: { name, contents },
      filePath,
      summary: envelope?.summary,
      additions: lineCount(contents),
      deletions: 0,
    };
  }

  return null;
}

function inferPatchPath(patch: string): string | undefined {
  const target = patch
    .split("\n")
    .find((line) => line.startsWith("+++ "))
    ?.slice(4)
    .trim();
  if (!target || target === "/dev/null") return undefined;
  return target.startsWith("b/") ? target.slice(2) : target;
}

export function buildToolDiffRows(presentation: ToolDiffPresentation): ToolDiffRow[] {
  const rows =
    presentation.kind === "patch"
      ? patchRows(presentation.patch)
      : fileRows(presentation.oldFile.contents, presentation.newFile.contents);
  return addInlineChangeRanges(rows);
}

/** Build the expensive row model once and derive both peek rows and count. */
export function buildToolDiffPeekModel(
  presentation: ToolDiffPresentation,
  maxRows = 12,
): ToolDiffPeekModel {
  const rows = buildToolDiffRows(presentation);
  const changed: ToolDiffRow[] = [];
  for (const row of rows) {
    if (row.kind === "addition" || row.kind === "deletion") {
      changed.push(row);
    }
  }
  return {
    rows:
      rows.length <= maxRows
        ? rows
        : changed.length > 0
          ? changed.slice(0, maxRows)
          : rows.slice(0, maxRows),
    changedRowCount: changed.length,
  };
}

function patchRows(patch: string): ToolDiffRow[] {
  const rows: ToolDiffRow[] = [];
  let oldLine: number | undefined;
  let newLine: number | undefined;

  for (const line of patch.trimEnd().split("\n")) {
    const hunk = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(line);
    if (hunk) {
      oldLine = Number(hunk[1]);
      newLine = Number(hunk[2]);
      rows.push({ kind: "metadata", content: line, prefix: "" });
      continue;
    }
    if (
      line.startsWith("---") ||
      line.startsWith("+++") ||
      line.startsWith("diff ") ||
      line.startsWith("index ") ||
      line.startsWith("\\ No newline")
    ) {
      rows.push({ kind: "metadata", content: line, prefix: "" });
      continue;
    }
    if (line.startsWith("-")) {
      rows.push({ kind: "deletion", content: line.slice(1), prefix: "-", oldLine });
      if (oldLine !== undefined) oldLine += 1;
      continue;
    }
    if (line.startsWith("+")) {
      rows.push({ kind: "addition", content: line.slice(1), prefix: "+", newLine });
      if (newLine !== undefined) newLine += 1;
      continue;
    }

    const content = line.startsWith(" ") ? line.slice(1) : line;
    rows.push({ kind: "context", content, prefix: " ", oldLine, newLine });
    if (oldLine !== undefined) oldLine += 1;
    if (newLine !== undefined) newLine += 1;
  }
  return rows;
}

function fileRows(oldContents: string, newContents: string): ToolDiffRow[] {
  const rows: ToolDiffRow[] = [];
  let oldLine = 1;
  let newLine = 1;

  for (const change of diffLines(oldContents, newContents)) {
    for (const content of splitLines(change.value)) {
      if (change.removed) {
        rows.push({ kind: "deletion", content, prefix: "-", oldLine });
        oldLine += 1;
      } else if (change.added) {
        rows.push({ kind: "addition", content, prefix: "+", newLine });
        newLine += 1;
      } else {
        rows.push({ kind: "context", content, prefix: " ", oldLine, newLine });
        oldLine += 1;
        newLine += 1;
      }
    }
  }
  return rows;
}

function splitLines(value: string): string[] {
  if (!value) return [];
  const lines = value.split("\n");
  if (lines.at(-1) === "") lines.pop();
  return lines;
}

function addInlineChangeRanges(rows: ToolDiffRow[]): ToolDiffRow[] {
  const result = rows.map((row) => ({ ...row }));
  let index = 0;

  while (index < result.length) {
    if (result[index]?.kind !== "deletion") {
      index += 1;
      continue;
    }
    const deletions: number[] = [];
    while (result[index]?.kind === "deletion") deletions.push(index++);
    const additions: number[] = [];
    while (result[index]?.kind === "addition") additions.push(index++);

    const pairs = Math.min(deletions.length, additions.length);
    for (let pair = 0; pair < pairs; pair += 1) {
      const deletion = result[deletions[pair]!]!;
      const addition = result[additions[pair]!]!;
      const ranges = changedRanges(deletion.content, addition.content);
      deletion.changedRanges = ranges.old;
      addition.changedRanges = ranges.new;
    }
  }
  return result;
}

function changedRanges(oldValue: string, newValue: string): {
  old: ToolDiffRange[];
  new: ToolDiffRange[];
} {
  const oldRanges: ToolDiffRange[] = [];
  const newRanges: ToolDiffRange[] = [];
  let oldOffset = 0;
  let newOffset = 0;

  for (const change of diffWordsWithSpace(oldValue, newValue)) {
    const length = change.value.length;
    if (change.removed) {
      oldRanges.push({ start: oldOffset, end: oldOffset + length });
      oldOffset += length;
    } else if (change.added) {
      newRanges.push({ start: newOffset, end: newOffset + length });
      newOffset += length;
    } else {
      oldOffset += length;
      newOffset += length;
    }
  }
  return { old: oldRanges, new: newRanges };
}

function firstString(...values: unknown[]): string | undefined {
  return values.find((value): value is string => typeof value === "string");
}

function lineCount(value: string): number {
  if (!value) return 0;
  return value.endsWith("\n")
    ? Math.max(0, value.split("\n").length - 1)
    : value.split("\n").length;
}

function rawUnifiedPatch(output?: string): string | undefined {
  if (!output) return undefined;
  const trimmed = output.trimStart();
  return trimmed.startsWith("--- ") && trimmed.includes("\n+++ ")
    ? output
    : undefined;
}

function parseToolEnvelope(
  output?: string,
): { diff?: string; summary?: string } | null {
  if (!output) return null;
  try {
    const parsed = JSON.parse(output) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return null;
    }
    const data =
      parsed.data && typeof parsed.data === "object" && !Array.isArray(parsed.data)
        ? (parsed.data as Record<string, unknown>)
        : undefined;
    return {
      diff: firstString(parsed.diff),
      summary: firstString(data?.message, parsed.summary),
    };
  } catch {
    return null;
  }
}

function countPatchChanges(patch: string): {
  additions: number;
  deletions: number;
} {
  let additions = 0;
  let deletions = 0;
  for (const line of patch.split("\n")) {
    if (line.startsWith("+") && !line.startsWith("+++")) additions += 1;
    if (line.startsWith("-") && !line.startsWith("---")) deletions += 1;
  }
  return { additions, deletions };
}

/** Convert Mitsuro's apply_patch envelope into a display-only unified patch. */
function convertApplyPatchToUnified(patch?: string): string | undefined {
  if (!patch?.includes("*** Begin Patch")) return undefined;

  const lines = patch.split("\n");
  const output: string[] = [];
  let index = 0;

  while (index < lines.length) {
    const header = lines[index]?.trim() ?? "";
    const updatePath = header.startsWith("*** Update File: ")
      ? header.slice("*** Update File: ".length).trim()
      : undefined;
    const addPath = header.startsWith("*** Add File: ")
      ? header.slice("*** Add File: ".length).trim()
      : undefined;

    if (!updatePath && !addPath) {
      index += 1;
      continue;
    }

    index += 1;
    const body: string[] = [];
    while (index < lines.length && !lines[index]?.trim().startsWith("*** ")) {
      const line = lines[index] ?? "";
      if (!line.startsWith("@@")) {
        body.push(line.startsWith("+") || line.startsWith("-") || line.startsWith(" ") ? line : ` ${line}`);
      }
      index += 1;
    }

    const additions = body.filter((line) => line.startsWith("+")).length;
    const deletions = body.filter((line) => line.startsWith("-")).length;
    const context = body.length - additions - deletions;
    const path = updatePath ?? addPath ?? "file";
    output.push(
      addPath ? "--- /dev/null" : `--- a/${path}`,
      `+++ b/${path}`,
      `@@ -${addPath ? 0 : 1},${addPath ? 0 : deletions + context} +1,${additions + context} @@`,
      ...body,
    );
  }

  return output.length > 0 ? `${output.join("\n")}\n` : undefined;
}

export function buildReadFilePresentation(
  toolCall: ToolCall,
): ToolDiffPresentation | null {
  const args = toolCall.arguments ?? {};
  const filePath = firstString(args.file_path, args.path) || "file";
  const content = extractReadContent(toolCall.output);
  if (content === undefined) return null;
  return {
    kind: "files",
    oldFile: { name: filePath, contents: content },
    newFile: { name: filePath, contents: content },
    filePath,
    summary: "file contents",
    additions: 0,
    deletions: 0,
  };
}

function extractReadContent(output?: string): string | undefined {
  if (!output) return undefined;
  const trimmed = output.trim();
  if (!trimmed) return "";
  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const data =
        parsed.data && typeof parsed.data === "object" && !Array.isArray(parsed.data)
          ? (parsed.data as Record<string, unknown>)
          : parsed;
      const content = firstString(data.content, data.text, data.output);
      if (content !== undefined) return content;
    }
  } catch {
    // plain text output
  }
  return output;
}
