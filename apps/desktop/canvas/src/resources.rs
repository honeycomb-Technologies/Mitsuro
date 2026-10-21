use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use gpui::prelude::FluentBuilder as _;
use gpui::{
    div, px, size, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, SharedString, StatefulInteractiveElement as _, Styled as _, Task,
    Window,
};
use gpui_component::button::Button;
use gpui_component::list::ListItem;
use gpui_component::text::TextView;
use gpui_component::tree::{Tree, TreeItem, TreeState};
use gpui_component::v_virtual_list;
use mitsuro_client::{
    FileResponse, FileTreeEntry, FileTreeResponse, GitChangedFile, GitChangesResponse,
    GitFileDiffResponse, MitsuroClient,
};
use tokio::runtime::Runtime;

use crate::theme::CanvasPalette;

const RESOURCE_QUEUE_CAPACITY: usize = 32;
const MAX_RENDERED_CHANGED_FILES: usize = 2_000;
const MAX_SPLIT_DIFF_ROWS: usize = 2_000;
const CHANGED_FILE_ROW_HEIGHT: f32 = 30.0;

enum FilesEvent {
    Tree {
        request_generation: u64,
        result: std::result::Result<FileTreeResponse, String>,
    },
    SelectFile(String),
    File {
        request_generation: u64,
        result: std::result::Result<FileResponse, String>,
    },
}

pub struct FilesSurface {
    client: MitsuroClient,
    runtime: Arc<Runtime>,
    palette: CanvasPalette,
    canvas_id: String,
    tree: Entity<TreeState>,
    selected_path: Option<String>,
    content: Option<String>,
    error: Option<String>,
    loading: bool,
    tx: flume::Sender<FilesEvent>,
    _listener: Task<()>,
    content_revision: u64,
    tree_request_generation: u64,
    file_request_generation: u64,
}

impl FilesSurface {
    pub fn new(
        client: MitsuroClient,
        runtime: Arc<Runtime>,
        canvas_id: String,
        palette: CanvasPalette,
        cx: &mut Context<Self>,
    ) -> Self {
        let tree = cx.new(|cx| TreeState::new(cx));
        let (tx, rx) = flume::bounded(RESOURCE_QUEUE_CAPACITY);
        let listener = cx.spawn(async move |this, cx| {
            while let Ok(event) = rx.recv_async().await {
                if this
                    .update(cx, |surface, cx| {
                        surface.reduce(event, cx);
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let mut surface = Self {
            client,
            runtime,
            palette,
            canvas_id,
            tree,
            selected_path: None,
            content: None,
            error: None,
            loading: true,
            tx,
            _listener: listener,
            content_revision: 0,
            tree_request_generation: 0,
            file_request_generation: 0,
        };
        surface.load_tree();
        surface
    }

    pub fn set_palette(&mut self, palette: CanvasPalette, cx: &mut Context<Self>) {
        if self.palette != palette {
            self.palette = palette;
            cx.notify();
        }
    }

    pub fn is_loading(&self) -> bool {
        self.loading
    }

    pub fn needs_attention(&self) -> bool {
        self.error.is_some()
    }

    fn load_tree(&mut self) {
        let client = self.client.clone();
        let canvas_id = self.canvas_id.clone();
        let tx = self.tx.clone();
        self.tree_request_generation = self.tree_request_generation.wrapping_add(1);
        let request_generation = self.tree_request_generation;
        self.runtime.spawn(async move {
            let result = client
                .canvas_file_tree(&canvas_id, 6)
                .await
                .map_err(|error| format!("Could not load project files: {error:#}"));
            let _ = tx
                .send_async(FilesEvent::Tree {
                    request_generation,
                    result,
                })
                .await;
        });
    }

    fn load_file(&mut self, path: String) {
        let client = self.client.clone();
        let tx = self.tx.clone();
        self.selected_path = Some(path.clone());
        self.loading = true;
        self.error = None;
        self.file_request_generation = self.file_request_generation.wrapping_add(1);
        let request_generation = self.file_request_generation;
        let canvas_id = self.canvas_id.clone();
        self.runtime.spawn(async move {
            let result = client
                .canvas_read_file(&canvas_id, &path)
                .await
                .map_err(|error| format!("Could not read {path}: {error:#}"));
            let _ = tx
                .send_async(FilesEvent::File {
                    request_generation,
                    result,
                })
                .await;
        });
    }

    fn reduce(&mut self, event: FilesEvent, cx: &mut Context<Self>) {
        match event {
            FilesEvent::Tree {
                request_generation, ..
            } if !is_current_response(request_generation, self.tree_request_generation) => {}
            FilesEvent::Tree {
                result: Ok(tree), ..
            } => {
                let items = tree
                    .entries
                    .into_iter()
                    .map(file_tree_item)
                    .collect::<Vec<_>>();
                self.tree.update(cx, |state, cx| state.set_items(items, cx));
                self.loading = false;
            }
            FilesEvent::Tree {
                result: Err(error), ..
            } => {
                self.error = Some(error);
                self.loading = false;
            }
            FilesEvent::SelectFile(path) => self.load_file(path),
            FilesEvent::File {
                request_generation, ..
            } if !is_current_response(request_generation, self.file_request_generation) => {}
            FilesEvent::File {
                result: Ok(file), ..
            } => {
                self.selected_path = Some(file.path);
                self.content = Some(file.content);
                self.content_revision = self.content_revision.wrapping_add(1);
                self.loading = false;
            }
            FilesEvent::File {
                result: Err(error), ..
            } => {
                self.error = Some(error);
                self.loading = false;
            }
        }
    }
}

fn is_current_response(response_generation: u64, current_generation: u64) -> bool {
    response_generation == current_generation
}

impl Render for FilesSurface {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.palette;
        let tx = self.tx.clone();
        let path = self.selected_path.clone();
        let markdown = self
            .content
            .as_deref()
            .map(|content| source_markdown(path.as_deref(), content));
        let revision = self.content_revision;
        div()
            .flex()
            .size_full()
            .min_h_0()
            .bg(palette.surface)
            .child(
                div()
                    .w(px(220.0))
                    .min_h_0()
                    .border_r_1()
                    .border_color(palette.edge)
                    .child(
                        Tree::new(&self.tree, move |index, entry, selected, _, _| {
                            let item = entry.item();
                            let id = item.id.clone();
                            let is_folder = entry.is_folder();
                            let sender = tx.clone();
                            ListItem::new(index)
                                .pl(px(10.0 + entry.depth() as f32 * 14.0))
                                .selected(selected)
                                .child(format!(
                                    "{}{}",
                                    if is_folder {
                                        if entry.is_expanded() {
                                            "▾ "
                                        } else {
                                            "▸ "
                                        }
                                    } else {
                                        "  "
                                    },
                                    item.label
                                ))
                                .on_click(move |_, _, _| {
                                    if !is_folder {
                                        let _ =
                                            sender.try_send(FilesEvent::SelectFile(id.to_string()));
                                    }
                                })
                        })
                        .size_full(),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_w_0()
                    .min_h_0()
                    .child(
                        div()
                            .h(px(32.0))
                            .px(px(12.0))
                            .flex()
                            .items_center()
                            .border_b_1()
                            .border_color(palette.edge)
                            .text_xs()
                            .text_color(palette.text_muted)
                            .child(path.unwrap_or_else(|| {
                                if self.loading {
                                    "Loading project files…".to_owned()
                                } else {
                                    "Select a file".to_owned()
                                }
                            })),
                    )
                    .when_some(self.error.clone(), |surface, error| {
                        surface.child(div().p(px(14.0)).text_color(palette.danger).child(error))
                    })
                    .when_some(markdown, |surface, markdown| {
                        surface.child(
                            div()
                                .id("files-content-scroll")
                                .flex_1()
                                .min_h_0()
                                .overflow_y_scroll()
                                .p(px(14.0))
                                .child(
                                    TextView::markdown(
                                        ("file-content", revision),
                                        markdown,
                                        window,
                                        cx,
                                    )
                                    .selectable(true),
                                ),
                        )
                    }),
            )
    }
}

fn file_tree_item(entry: FileTreeEntry) -> TreeItem {
    let expanded = entry.path.split('/').count() <= 2;
    TreeItem::new(entry.path, entry.name)
        .expanded(expanded)
        .children(
            entry
                .children
                .unwrap_or_default()
                .into_iter()
                .map(file_tree_item),
        )
}

fn source_markdown(path: Option<&str>, content: &str) -> String {
    format!("````{}\n{content}\n````", source_language(path))
}

fn source_language(path: Option<&str>) -> &'static str {
    let Some(extension) = path
        .and_then(|path| Path::new(path).extension())
        .and_then(|extension| extension.to_str())
    else {
        return "text";
    };
    if extension.eq_ignore_ascii_case("rs") {
        "rust"
    } else if extension.eq_ignore_ascii_case("ts") {
        "typescript"
    } else if extension.eq_ignore_ascii_case("tsx") {
        "tsx"
    } else if extension.eq_ignore_ascii_case("js") {
        "javascript"
    } else if extension.eq_ignore_ascii_case("jsx") {
        "jsx"
    } else if extension.eq_ignore_ascii_case("json") {
        "json"
    } else if extension.eq_ignore_ascii_case("toml") {
        "toml"
    } else if extension.eq_ignore_ascii_case("yaml") || extension.eq_ignore_ascii_case("yml") {
        "yaml"
    } else if extension.eq_ignore_ascii_case("md") || extension.eq_ignore_ascii_case("markdown") {
        "markdown"
    } else if extension.eq_ignore_ascii_case("py") {
        "python"
    } else if extension.eq_ignore_ascii_case("sh") || extension.eq_ignore_ascii_case("bash") {
        "bash"
    } else if extension.eq_ignore_ascii_case("html") {
        "html"
    } else if extension.eq_ignore_ascii_case("css") {
        "css"
    } else if extension.eq_ignore_ascii_case("sql") {
        "sql"
    } else if extension.eq_ignore_ascii_case("go") {
        "go"
    } else if extension.eq_ignore_ascii_case("swift") {
        "swift"
    } else if extension.eq_ignore_ascii_case("kt") || extension.eq_ignore_ascii_case("kts") {
        "kotlin"
    } else if matches!(extension.to_ascii_lowercase().as_str(), "c" | "h") {
        "c"
    } else if matches!(
        extension.to_ascii_lowercase().as_str(),
        "cc" | "cpp" | "cxx" | "hh" | "hpp" | "hxx"
    ) {
        "cpp"
    } else {
        "text"
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiffMode {
    Unified,
    Split,
}

enum ChangesEvent {
    Changes {
        request_generation: u64,
        result: std::result::Result<GitChangesResponse, String>,
    },
    Diff {
        request_generation: u64,
        result: std::result::Result<GitFileDiffResponse, String>,
    },
}

pub struct ChangesSurface {
    client: MitsuroClient,
    runtime: Arc<Runtime>,
    palette: CanvasPalette,
    project_dir: String,
    files: Vec<GitChangedFile>,
    files_truncated: bool,
    selected_path: Option<String>,
    diff: Option<GitFileDiffResponse>,
    mode: DiffMode,
    loading: bool,
    error: Option<String>,
    tx: flume::Sender<ChangesEvent>,
    _listener: Task<()>,
    diff_revision: u64,
    changes_request_generation: u64,
    diff_request_generation: u64,
}

impl ChangesSurface {
    pub fn new(
        client: MitsuroClient,
        runtime: Arc<Runtime>,
        project_dir: String,
        palette: CanvasPalette,
        cx: &mut Context<Self>,
    ) -> Self {
        let (tx, rx) = flume::bounded(RESOURCE_QUEUE_CAPACITY);
        let listener = cx.spawn(async move |this, cx| {
            while let Ok(event) = rx.recv_async().await {
                if this
                    .update(cx, |surface, cx| {
                        surface.reduce(event);
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let mut surface = Self {
            client,
            runtime,
            palette,
            project_dir,
            files: Vec::new(),
            files_truncated: false,
            selected_path: None,
            diff: None,
            mode: DiffMode::Unified,
            loading: true,
            error: None,
            tx,
            _listener: listener,
            diff_revision: 0,
            changes_request_generation: 0,
            diff_request_generation: 0,
        };
        surface.load_changes();
        surface
    }

    pub fn set_palette(&mut self, palette: CanvasPalette, cx: &mut Context<Self>) {
        if self.palette != palette {
            self.palette = palette;
            cx.notify();
        }
    }

    pub fn is_loading(&self) -> bool {
        self.loading
    }

    pub fn needs_attention(&self) -> bool {
        self.error.is_some()
    }

    fn load_changes(&mut self) {
        let client = self.client.clone();
        let project = self.project_dir.clone();
        let tx = self.tx.clone();
        self.changes_request_generation = self.changes_request_generation.wrapping_add(1);
        let request_generation = self.changes_request_generation;
        self.runtime.spawn(async move {
            let result = client
                .git_changes(&project)
                .await
                .map_err(|error| format!("Could not load Git changes: {error:#}"));
            let _ = tx
                .send_async(ChangesEvent::Changes {
                    request_generation,
                    result,
                })
                .await;
        });
    }

    fn select_file(&mut self, path: String) {
        let client = self.client.clone();
        let project = self.project_dir.clone();
        let tx = self.tx.clone();
        self.selected_path = Some(path.clone());
        self.loading = true;
        self.error = None;
        self.diff_request_generation = self.diff_request_generation.wrapping_add(1);
        let request_generation = self.diff_request_generation;
        self.runtime.spawn(async move {
            let result = client
                .git_file_diff(&project, &path)
                .await
                .map_err(|error| format!("Could not load diff for {path}: {error:#}"));
            let _ = tx
                .send_async(ChangesEvent::Diff {
                    request_generation,
                    result,
                })
                .await;
        });
    }

    fn reduce(&mut self, event: ChangesEvent) {
        match event {
            ChangesEvent::Changes {
                request_generation, ..
            } if !is_current_response(request_generation, self.changes_request_generation) => {}
            ChangesEvent::Changes {
                result: Ok(changes),
                ..
            } => {
                (self.files, self.files_truncated) = bounded_changed_files(changes);
                self.loading = false;
            }
            ChangesEvent::Changes {
                result: Err(error), ..
            } => {
                self.error = Some(error);
                self.loading = false;
            }
            ChangesEvent::Diff {
                request_generation, ..
            } if !is_current_response(request_generation, self.diff_request_generation) => {}
            ChangesEvent::Diff {
                result: Err(error), ..
            } => {
                self.error = Some(error);
                self.loading = false;
            }
            ChangesEvent::Diff {
                result: Ok(diff), ..
            } => {
                self.selected_path = Some(diff.path.clone());
                self.diff = Some(diff);
                self.diff_revision = self.diff_revision.wrapping_add(1);
                self.loading = false;
            }
        }
    }

    fn render_split(&self) -> impl IntoElement {
        let palette = self.palette;
        let rows = self
            .diff
            .as_ref()
            .map(|diff| bounded_split_patch_rows(&diff.patch).0)
            .unwrap_or_default();
        div()
            .flex()
            .flex_col()
            .font_family("monospace")
            .text_sm()
            .children(rows.into_iter().map(|(left, right)| {
                div()
                    .flex()
                    .border_b_1()
                    .border_color(palette.edge)
                    .child(
                        div()
                            .w_1_2()
                            .min_w_0()
                            .px(px(8.0))
                            .py(px(2.0))
                            .text_color(
                                if left.as_deref().is_some_and(|line| line.starts_with('-')) {
                                    palette.danger
                                } else {
                                    palette.text_muted
                                },
                            )
                            .child(left.unwrap_or_default()),
                    )
                    .child(
                        div()
                            .w_1_2()
                            .min_w_0()
                            .px(px(8.0))
                            .py(px(2.0))
                            .border_l_1()
                            .border_color(palette.edge)
                            .text_color(
                                if right.as_deref().is_some_and(|line| line.starts_with('+')) {
                                    palette.pulse
                                } else {
                                    palette.text_muted
                                },
                            )
                            .child(right.unwrap_or_default()),
                    )
            }))
    }
}

fn bounded_changed_files(mut changes: GitChangesResponse) -> (Vec<GitChangedFile>, bool) {
    let locally_truncated = changes.files.len() > MAX_RENDERED_CHANGED_FILES;
    changes.files.truncate(MAX_RENDERED_CHANGED_FILES);
    (changes.files, changes.truncated || locally_truncated)
}

fn diff_notice_messages(diff: &GitFileDiffResponse) -> Vec<&'static str> {
    let mut notices = Vec::with_capacity(2);
    if diff.binary {
        notices.push("Binary file — textual diff is unavailable.");
    }
    if diff.truncated {
        notices.push("Diff truncated — only the bounded prefix is shown.");
    }
    notices
}

impl Render for ChangesSurface {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.palette;
        let file_item_sizes = Rc::new(vec![
            size(px(230.0), px(CHANGED_FILE_ROW_HEIGHT));
            self.files.len()
        ]);
        let files_truncated = self.files_truncated;
        let diff = self.diff.clone();
        let diff_notices = diff.as_ref().map(diff_notice_messages).unwrap_or_default();
        let mode = self.mode;
        let revision = self.diff_revision;
        div()
            .flex()
            .size_full()
            .min_h_0()
            .bg(palette.surface)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .w(px(230.0))
                    .min_h_0()
                    .border_r_1()
                    .border_color(palette.edge)
                    .when(files_truncated, |list| {
                        list.child(
                            div()
                                .px(px(8.0))
                                .py(px(6.0))
                                .border_b_1()
                                .border_color(palette.edge)
                                .text_xs()
                                .text_color(palette.brass)
                                .child(
                                    "Changed-file list truncated; additional paths are omitted.",
                                ),
                        )
                    })
                    .child(
                        v_virtual_list(
                            cx.entity(),
                            "changes-file-list",
                            file_item_sizes,
                            |surface, range, _, cx| {
                                range
                                    .filter_map(|index| surface.files.get(index).cloned())
                                    .map(|file| {
                                        let path = file.path.clone();
                                        Button::new(SharedString::from(format!(
                                            "change-{}",
                                            file.path
                                        )))
                                        .label(format!(
                                            "{}  +{} −{}",
                                            file.path, file.additions, file.deletions
                                        ))
                                        .w_full()
                                        .h(px(CHANGED_FILE_ROW_HEIGHT))
                                        .on_click(
                                            cx.listener(move |surface, _, _, cx| {
                                                surface.select_file(path.clone());
                                                cx.notify();
                                            }),
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            },
                        )
                        .flex_1()
                        .min_h_0(),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_w_0()
                    .min_h_0()
                    .child(
                        div()
                            .h(px(36.0))
                            .px(px(8.0))
                            .flex()
                            .items_center()
                            .justify_between()
                            .border_b_1()
                            .border_color(palette.edge)
                            .child(
                                div().text_xs().text_color(palette.text_muted).child(
                                    self.selected_path
                                        .clone()
                                        .unwrap_or_else(|| "Select a changed file".to_owned()),
                                ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap(px(4.0))
                                    .child(
                                        Button::new("diff-unified")
                                            .label("Unified")
                                            .compact()
                                            .on_click(cx.listener(|surface, _, _, cx| {
                                                surface.mode = DiffMode::Unified;
                                                cx.notify();
                                            })),
                                    )
                                    .child(
                                        Button::new("diff-split")
                                            .label("Split")
                                            .compact()
                                            .on_click(cx.listener(|surface, _, _, cx| {
                                                surface.mode = DiffMode::Split;
                                                cx.notify();
                                            })),
                                    ),
                            ),
                    )
                    .when_some(self.error.clone(), |surface, error| {
                        surface.child(div().p(px(14.0)).text_color(palette.danger).child(error))
                    })
                    .children(diff_notices.into_iter().map(|notice| {
                        div()
                            .px(px(10.0))
                            .py(px(6.0))
                            .border_b_1()
                            .border_color(palette.edge)
                            .text_xs()
                            .text_color(palette.brass)
                            .child(notice)
                    }))
                    .when_some(diff, |surface, diff| match mode {
                        DiffMode::Unified => surface.child(
                            div()
                                .id("changes-unified-scroll")
                                .flex_1()
                                .min_h_0()
                                .overflow_y_scroll()
                                .p(px(10.0))
                                .child(
                                    TextView::markdown(
                                        ("git-diff", revision),
                                        format!("````diff\n{}\n````", diff.patch),
                                        window,
                                        cx,
                                    )
                                    .selectable(true),
                                ),
                        ),
                        DiffMode::Split => surface.child(
                            div()
                                .id("changes-split-scroll")
                                .flex_1()
                                .min_h_0()
                                .overflow_y_scroll()
                                .child(self.render_split()),
                        ),
                    }),
            )
    }
}

fn split_patch_rows(patch: &str) -> Vec<(Option<String>, Option<String>)> {
    let mut rows = Vec::new();
    let mut pending_deletions = Vec::<String>::new();
    for line in patch.lines() {
        if line.starts_with("---") || line.starts_with("+++") {
            rows.push((Some(line.to_owned()), Some(line.to_owned())));
        } else if line.starts_with('-') {
            pending_deletions.push(line.to_owned());
        } else if line.starts_with('+') {
            let left = (!pending_deletions.is_empty()).then(|| pending_deletions.remove(0));
            rows.push((left, Some(line.to_owned())));
        } else {
            rows.extend(pending_deletions.drain(..).map(|line| (Some(line), None)));
            rows.push((Some(line.to_owned()), Some(line.to_owned())));
        }
    }
    rows.extend(pending_deletions.into_iter().map(|line| (Some(line), None)));
    rows
}

fn bounded_split_patch_rows(patch: &str) -> (Vec<(Option<String>, Option<String>)>, bool) {
    let mut rows = split_patch_rows(patch);
    let truncated = rows.len() > MAX_SPLIT_DIFF_ROWS;
    if truncated {
        rows.truncate(MAX_SPLIT_DIFF_ROWS);
    }
    (rows, truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_diff_pairs_replacements_and_keeps_context() {
        let rows = split_patch_rows("@@ -1 +1 @@\n-old\n+new\n context");
        assert_eq!(
            rows,
            vec![
                (
                    Some("@@ -1 +1 @@".to_owned()),
                    Some("@@ -1 +1 @@".to_owned())
                ),
                (Some("-old".to_owned()), Some("+new".to_owned())),
                (Some(" context".to_owned()), Some(" context".to_owned())),
            ]
        );
    }

    #[test]
    fn split_diff_rows_are_bounded_for_virtualized_rendering() {
        let patch = (0..=MAX_SPLIT_DIFF_ROWS)
            .map(|index| format!(" context {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let (rows, truncated) = bounded_split_patch_rows(&patch);
        assert_eq!(rows.len(), MAX_SPLIT_DIFF_ROWS);
        assert!(truncated);
    }

    #[test]
    fn source_markdown_uses_extension_language_and_long_fence() {
        assert!(source_markdown(Some("src/lib.rs"), "``` nested").starts_with("````rust"));
        assert!(source_markdown(Some("web/App.tsx"), "const app = 1;").starts_with("````tsx"));
        assert!(source_markdown(Some("NOTICE.unknown"), "plain").starts_with("````text"));
    }

    #[test]
    fn changed_file_list_is_capped_and_preserves_server_truncation() {
        let files = (0..=MAX_RENDERED_CHANGED_FILES)
            .map(|index| GitChangedFile {
                path: format!("src/file-{index}.rs"),
                status: "modified".to_owned(),
                additions: 1,
                deletions: 0,
            })
            .collect();
        let (files, truncated) = bounded_changed_files(GitChangesResponse {
            in_repo: true,
            repo_root: Some("/repo".to_owned()),
            files,
            truncated: false,
        });
        assert_eq!(files.len(), MAX_RENDERED_CHANGED_FILES);
        assert!(truncated);

        let (_, server_truncated) = bounded_changed_files(GitChangesResponse {
            in_repo: true,
            repo_root: Some("/repo".to_owned()),
            files: Vec::new(),
            truncated: true,
        });
        assert!(server_truncated);
    }

    #[test]
    fn binary_and_truncated_diff_notices_apply_to_both_modes() {
        let diff = GitFileDiffResponse {
            path: "asset.bin".to_owned(),
            patch: String::new(),
            truncated: true,
            binary: true,
        };
        for _mode in [DiffMode::Unified, DiffMode::Split] {
            assert_eq!(
                diff_notice_messages(&diff),
                vec![
                    "Binary file — textual diff is unavailable.",
                    "Diff truncated — only the bounded prefix is shown."
                ]
            );
        }
    }

    #[test]
    fn stale_file_responses_are_rejected_by_generation() {
        assert!(!is_current_response(4, 5));
        assert!(is_current_response(5, 5));
    }

    #[test]
    fn stale_diff_responses_are_rejected_by_generation() {
        let first_selection_generation = 19;
        let current_selection_generation = 20;
        assert!(!is_current_response(
            first_selection_generation,
            current_selection_generation
        ));
        assert!(is_current_response(
            current_selection_generation,
            current_selection_generation
        ));
    }
}
