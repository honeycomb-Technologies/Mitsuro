#[cfg(unix)]
use super::execution::execute_foreground;
use super::execution::{join_reader_with_timeout, BoundedOutputBuffer};
#[cfg(unix)]
use super::shell::{build_shell_command, configure_foreground_process_group};
use super::shell::{
    contains_embedded_background_operator, normalize_tracked_background_command,
    strip_shell_background_suffix,
};
#[cfg(target_os = "linux")]
use super::workspace_only_shell_command;
use super::{
    background_endpoint_hints, normalize_tailscale_serve_result, output_spool_path,
    sandboxed_shell_command, BashTool,
};
use crate::process::{CommandEnvironmentPolicy, ProcessRegistry};
use crate::tools::registry::{ShellIsolationPolicy, Tool};
use crate::tools::{ToolContext, ToolResult};
use serde_json::json;
use std::sync::Arc;

#[cfg(unix)]
use std::process::Stdio;
#[cfg(unix)]
use std::time::Duration;

#[test]
fn tailscale_operator_denial_overrides_masked_compound_success() {
    let result = ToolResult::success_data(json!({
        "output": "sending serve config: 401 Unauthorized: must be root, or be an operator and able to run 'sudo tailscale' to serve a path\nafter-command-ok"
    }));

    let normalized = normalize_tailscale_serve_result(
        "tailscale serve --bg --https=9443 http://127.0.0.1:5180; echo after-command-ok",
        result,
    );

    assert!(normalized.is_error, "{}", normalized.output);
    let envelope: serde_json::Value = serde_json::from_str(&normalized.output).expect("tool JSON");
    assert_eq!(
        envelope["error"]["code"].as_str(),
        Some("tailscale_operator_required")
    );
    assert_eq!(
        envelope["data"]["status"].as_str(),
        Some("operator_required")
    );
    assert!(envelope["data"]["next_action"]
        .as_str()
        .is_some_and(|message| message.contains("Do not retry with sudo")));
}

#[test]
fn unrelated_permission_text_is_not_reclassified_as_tailscale_serve() {
    let result = ToolResult::success_data(json!({
        "output": "401 Unauthorized: must be root, or be an operator"
    }));

    let normalized = normalize_tailscale_serve_result("curl https://example.com", result);
    assert!(!normalized.is_error, "{}", normalized.output);
}

#[cfg(unix)]
struct TestProcessCleanup {
    pids: Vec<u32>,
    armed: bool,
}

#[cfg(unix)]
impl TestProcessCleanup {
    fn new(pids: Vec<u32>) -> Self {
        Self { pids, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(unix)]
impl Drop for TestProcessCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        for pid in &self.pids {
            if let Ok(pid) = libc::pid_t::try_from(*pid) {
                // Test-only best effort: every process is uniquely created by
                // this test and is killed individually if an assertion fails.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        if stat
            .rsplit_once(')')
            .and_then(|(_, suffix)| suffix.split_whitespace().next())
            == Some("Z")
        {
            // The process has exited; this container's PID 1 may reap the
            // orphaned zombie later, but it cannot execute any more work.
            return false;
        }
    }

    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(unix)]
async fn wait_for_processes_to_exit(pids: &[u32]) -> bool {
    for _ in 0..200 {
        if pids.iter().all(|pid| !process_is_alive(*pid)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[cfg(unix)]
async fn wait_for_pid_file(path: &std::path::Path) -> (u32, u32) {
    for _ in 0..200 {
        if let Ok(raw) = std::fs::read_to_string(path) {
            let pids = raw
                .split_whitespace()
                .filter_map(|value| value.parse::<u32>().ok())
                .collect::<Vec<_>>();
            if pids.len() == 2 {
                return (pids[0], pids[1]);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("process tree did not write PID file {}", path.display());
}

#[cfg(unix)]
fn foreground_process_tree_command(
    working_dir: &std::path::Path,
    pid_file_name: &str,
) -> tokio::process::Command {
    let script_name = "foreground-process-tree.sh";
    #[cfg(target_os = "linux")]
    let child_command = "setsid sleep 60 &";
    #[cfg(not(target_os = "linux"))]
    let child_command = "sleep 60 &";
    std::fs::write(
        working_dir.join(script_name),
        format!(
            "#!/bin/sh\n{child_command}\nchild=$!\nprintf '%s %s\\n' \"$$\" \"$child\" > \"$1\"\nwait \"$child\"\n"
        ),
    )
    .expect("write process-tree script");

    let ctx = ToolContext {
        working_dir: working_dir.to_path_buf(),
        ..Default::default()
    };
    let mut command = build_shell_command(&format!("sh {script_name} {pid_file_name}"), &ctx);
    configure_foreground_process_group(&mut command);
    command.kill_on_drop(true);
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command
}

#[cfg(unix)]
#[tokio::test]
async fn foreground_shell_uses_the_sanitized_environment_contract() {
    let temp = tempfile::tempdir().expect("tempdir");
    let isolated_home = temp.path().join("isolated-home");
    let mut ctx = ToolContext {
        working_dir: temp.path().to_path_buf(),
        command_environment_policy: CommandEnvironmentPolicy::Sanitized,
        ..Default::default()
    };
    ctx.command_environment
        .insert("HOME".to_string(), isolated_home.display().to_string());
    ctx.command_environment.insert(
        "MITSURO_TEST_SECRET".to_string(),
        "must-not-escape".to_string(),
    );
    let output = build_shell_command(
        "printf '%s|%s' \"${MITSURO_TEST_SECRET-unset}\" \"$HOME\"",
        &ctx,
    )
    .output()
    .await
    .expect("run foreground command");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf8 output"),
        format!("unset|{}", isolated_home.display())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn foreground_project_shell_uses_scoped_writable_npm_cache() {
    let project = tempfile::tempdir().expect("project");
    let ctx = ToolContext {
        working_dir: project.path().to_path_buf(),
        project_dir: Some(project.path().to_path_buf()),
        ..Default::default()
    };

    let output = build_shell_command(
        "printf '%s|%s' \"$npm_config_cache\" \"$NPM_CONFIG_CACHE\"",
        &ctx,
    )
    .output()
    .await
    .expect("run project command");

    assert!(output.status.success());
    let output = String::from_utf8(output.stdout).expect("utf8 output");
    let (lower, upper) = output.split_once('|').expect("cache pair");
    assert_eq!(lower, upper);
    assert!(std::path::Path::new(lower).is_dir());
    assert!(!lower.starts_with(project.path().to_string_lossy().as_ref()));
}

#[test]
fn strip_shell_background_suffix_accepts_simple_suffix() {
    let parsed = strip_shell_background_suffix("npm run dev &");
    assert_eq!(parsed.as_deref(), Some("npm run dev"));
}

#[test]
fn strip_shell_background_suffix_rejects_quoted_ampersand() {
    let parsed = strip_shell_background_suffix("echo '&'");
    assert!(parsed.is_none());
}

#[test]
fn strip_shell_background_suffix_rejects_escaped_ampersand() {
    let parsed = strip_shell_background_suffix(r"echo foo \&");
    assert!(parsed.is_none());
}

#[test]
fn strip_shell_background_suffix_rejects_double_ampersand() {
    let parsed = strip_shell_background_suffix("echo hi &&");
    assert!(parsed.is_none());
}

#[test]
fn embedded_background_detection_distinguishes_jobs_from_redirects_and_logic() {
    assert!(contains_embedded_background_operator(
        "npm run build && (npm run preview > .preview.log 2>&1 & echo $!)"
    ));
    assert!(contains_embedded_background_operator("server & echo ready"));
    assert!(!contains_embedded_background_operator(
        "npm run build && printf ok 2>&1"
    ));
    assert!(!contains_embedded_background_operator(
        "printf '&' && echo ok"
    ));
    assert!(!contains_embedded_background_operator(
        "command &> output.log"
    ));
}

#[test]
fn tracked_background_command_removes_redundant_detachment_wrapper() {
    let (command, inferred, removed_wrapper) = normalize_tracked_background_command(
        "nohup python3 -m http.server 6180 --bind 127.0.0.1 > /dev/null 2>&1 &",
    );

    assert_eq!(command, "python3 -m http.server 6180 --bind 127.0.0.1");
    assert!(inferred);
    assert!(removed_wrapper);
}

#[test]
fn tracked_background_command_preserves_quoted_redirect_text() {
    let (command, inferred, removed_wrapper) =
        normalize_tracked_background_command("printf '%s' '> /dev/null 2>&1' &");

    assert_eq!(command, "printf '%s' '> /dev/null 2>&1'");
    assert!(inferred);
    assert!(!removed_wrapper);
}

#[test]
fn tracked_background_command_preserves_embedded_nohup_text() {
    let (command, inferred, removed_wrapper) =
        normalize_tracked_background_command("printf 'nohup server > /dev/null 2>&1'");

    assert_eq!(command, "printf 'nohup server > /dev/null 2>&1'");
    assert!(!inferred);
    assert!(!removed_wrapper);
}

#[test]
fn background_endpoint_hints_extract_common_loopback_forms() {
    assert_eq!(
        background_endpoint_hints("python3 server.py --host 127.0.0.1 --port 5940"),
        vec!["127.0.0.1:5940"]
    );
    assert_eq!(
        background_endpoint_hints("python3 -m http.server 5180 --bind localhost"),
        vec!["localhost:5180"]
    );
    assert_eq!(
        background_endpoint_hints("vite --host=127.0.0.1 --port=5173"),
        vec!["127.0.0.1:5173"]
    );
}

#[test]
fn bounded_output_buffer_keeps_recent_lines() {
    let mut buffer = BoundedOutputBuffer::new(3, 1024);
    buffer.push_line("l1");
    buffer.push_line("l2");
    buffer.push_line("l3");
    buffer.push_line("l4");

    let text = buffer.into_text();
    assert!(!text.contains("l1"));
    assert!(text.contains("l2"));
    assert!(text.contains("l3"));
    assert!(text.contains("l4"));
}

#[test]
fn bounded_output_buffer_clips_to_max_bytes() {
    let mut buffer = BoundedOutputBuffer::new(100, 10);
    buffer.push_line("12345");
    buffer.push_line("67890");
    buffer.push_line("abcdef");

    let text = buffer.into_text();
    assert!(text.len() <= 200);
    assert!(text.contains("abcdef") || text.contains("bcdef"));
}

#[tokio::test]
async fn join_reader_with_timeout_does_not_double_poll_completed_handle() {
    let handle = tokio::spawn(async {});
    join_reader_with_timeout(handle).await;
}

#[test]
fn output_spool_is_workspace_local_and_session_scoped() {
    let temp = tempfile::tempdir().expect("tempdir");
    let ctx = ToolContext {
        working_dir: temp.path().to_path_buf(),
        session_id: Some("session/with spaces".to_string()),
        ..Default::default()
    };

    let path = output_spool_path(&ctx);
    assert!(path.starts_with(temp.path().join(".mitsuro/tool-output")));
    assert!(path.to_string_lossy().contains("session_with_spaces"));
    assert_eq!(
        path.extension().and_then(|value| value.to_str()),
        Some("log")
    );
}

#[test]
fn output_spool_prefers_runtime_state_over_the_project_workspace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().join("workspace");
    let state_dir = temp.path().join("state");
    let ctx = ToolContext {
        working_dir: working_dir.clone(),
        db_path: Some(state_dir.join("mitsuro.db")),
        session_id: Some("session-1".to_string()),
        ..Default::default()
    };

    let path = output_spool_path(&ctx);
    assert!(path.starts_with(state_dir.join("tool-output/session-1")));
    assert!(!path.starts_with(working_dir));
}

fn changed_value(result: &crate::tools::ToolResult) -> Option<bool> {
    serde_json::from_str::<serde_json::Value>(&result.output)
        .ok()
        .and_then(|value| value.get("changed").and_then(serde_json::Value::as_bool))
}

#[cfg(unix)]
#[tokio::test]
async fn foreground_bash_reports_real_file_state_deltas() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let state_dir = temp.path().join("state");
    std::fs::create_dir(&state_dir).expect("state dir");
    let ctx = ToolContext {
        working_dir: working_dir.clone(),
        db_path: Some(state_dir.join("mitsuro.db")),
        ..Default::default()
    };

    let first_append = BashTool
        .execute(json!({"command": "printf x >> log"}), &ctx)
        .await;
    let second_append = BashTool
        .execute(json!({"command": "printf x >> log"}), &ctx)
        .await;
    assert!(!first_append.is_error, "{}", first_append.output);
    assert!(!second_append.is_error, "{}", second_append.output);
    assert_eq!(changed_value(&first_append), Some(true));
    assert_eq!(changed_value(&second_append), Some(true));
    assert_eq!(
        std::fs::read_to_string(working_dir.join("log")).unwrap(),
        "xx"
    );

    let first_overwrite = BashTool
        .execute(json!({"command": "printf stable > output"}), &ctx)
        .await;
    let repeated_overwrite = BashTool
        .execute(json!({"command": "printf stable > output"}), &ctx)
        .await;
    assert_eq!(changed_value(&first_overwrite), Some(true));
    assert_eq!(changed_value(&repeated_overwrite), None);
}

#[cfg(target_os = "linux")]
fn linux_bubblewrap_user_namespaces_work() -> bool {
    if !std::path::Path::new("/usr/bin/bwrap").is_file() {
        return false;
    }
    std::process::Command::new("/usr/bin/bwrap")
        .args([
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--",
            "true",
        ])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
#[test]
fn worker_goal_shell_command_uses_private_mount_network_and_pid_namespaces_only() {
    let workspace = tempfile::tempdir().expect("workspace");
    let working_dir = workspace
        .path()
        .canonicalize()
        .expect("canonical workspace");
    let strict = ToolContext {
        working_dir: working_dir.clone(),
        sandbox_root: Some(working_dir.clone()),
        shell_isolation_policy: ShellIsolationPolicy::WorkspaceOnly,
        ..Default::default()
    };
    let wrapped = sandboxed_shell_command("printf safe", &strict).expect("strict command");

    for required in [
        "--unshare-user",
        "--unshare-pid",
        "--unshare-net",
        "--unshare-ipc",
        "--hostname mitsuro-worker",
        "--cap-drop ALL",
        "--proc /proc",
    ] {
        assert!(wrapped.contains(required), "missing {required}: {wrapped}");
    }
    assert!(!wrapped.contains("--ro-bind / /"), "{wrapped}");
    assert!(wrapped.contains(&format!("--bind {0} {0}", working_dir.display())));

    let missing_root = ToolContext {
        shell_isolation_policy: ShellIsolationPolicy::WorkspaceOnly,
        ..Default::default()
    };
    assert!(
        sandboxed_shell_command("printf unsafe", &missing_root).is_err(),
        "strict isolation must fail closed without a configured workspace root"
    );

    let compatible = ToolContext {
        working_dir: working_dir.clone(),
        sandbox_root: Some(working_dir),
        ..Default::default()
    };
    let compatible_command =
        sandboxed_shell_command("printf compatible", &compatible).expect("compatible command");
    assert!(compatible_command.contains("--ro-bind / /"));
    assert!(!compatible_command.contains("--unshare-net"));
    assert!(!compatible_command.contains("--unshare-pid"));
}

#[cfg(target_os = "linux")]
#[test]
fn worker_goal_shell_rejects_root_and_system_runtime_workspaces() {
    for candidate in ["/", "/usr", "/bin", "/sbin", "/lib", "/lib64"] {
        let candidate = std::path::Path::new(candidate);
        if !candidate.exists() {
            continue;
        }
        let canonical = candidate.canonicalize().expect("canonical runtime root");
        let error = workspace_only_shell_command("printf unsafe", &canonical, &canonical)
            .expect_err("broad or runtime workspace root must fail closed");
        assert!(
            error.contains("refused unsafe workspace root"),
            "unexpected rejection for {}: {error}",
            candidate.display()
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn worker_goal_shell_hides_sensitive_env_host_files_processes_and_routes() {
    if !linux_bubblewrap_user_namespaces_work() {
        eprintln!(
            "skipping Worker Goal shell isolation test: bubblewrap user namespaces are unavailable"
        );
        return;
    }

    let parent = tempfile::tempdir().expect("temp parent");
    let working_dir = parent.path().join("assigned");
    let outside = parent.path().join("outside-secret.txt");
    std::fs::create_dir(&working_dir).expect("assigned workspace");
    std::fs::write(&outside, "must-not-be-visible").expect("outside canary");
    let working_dir = working_dir.canonicalize().expect("canonical workspace");
    let mut ctx = ToolContext {
        working_dir: working_dir.clone(),
        sandbox_root: Some(working_dir.clone()),
        command_environment_policy: CommandEnvironmentPolicy::Explicit,
        shell_isolation_policy: ShellIsolationPolicy::WorkspaceOnly,
        ..Default::default()
    };
    ctx.command_environment.insert(
        "PATH".to_string(),
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
    );
    ctx.command_environment
        .insert("HOME".to_string(), working_dir.display().to_string());
    ctx.command_environment.insert(
        "MITSURO_TEST_SECRET".to_string(),
        "must-not-escape".to_string(),
    );
    ctx.command_environment
        .insert("JAVA_HOME".to_string(), "/host/private/jdk".to_string());
    let outside = shell_words::quote(&outside.display().to_string()).into_owned();
    let host_pid = std::process::id();
    let command = format!(
        "outside=hidden; test -r {outside} && outside=visible; \
         etc=hidden; test -r /etc/passwd && etc=visible; \
         process=hidden; test -e /proc/{host_pid} && process=visible; \
         route=isolated; route_header=1; \
         while IFS= read -r _route_line; do \
           if [ \"$route_header\" = 1 ]; then route_header=0; else route=routed; break; fi; \
         done < /proc/net/route; \
         printf '%s|%s|%s|%s|%s|%s' \
           \"${{MITSURO_TEST_SECRET-unset}}\" \"${{JAVA_HOME-unset}}\" \
           \"$outside\" \"$etc\" \"$process\" \"$route\""
    );

    let result = BashTool.execute(json!({"command": command}), &ctx).await;
    assert!(!result.is_error, "{}", result.output);
    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    assert_eq!(
        envelope["data"]["output"].as_str().map(str::trim),
        Some("unset|unset|hidden|hidden|hidden|isolated")
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn delegated_shell_sandbox_blocks_absolute_writes_outside_its_root() {
    if !linux_bubblewrap_user_namespaces_work() {
        eprintln!(
            "skipping delegated shell sandbox test: bubblewrap user namespaces are unavailable"
        );
        return;
    }

    let parent = tempfile::tempdir().expect("temp parent");
    let working_dir = parent.path().join("assigned");
    let outside = parent.path().join("outside.txt");
    std::fs::create_dir(&working_dir).expect("assigned workspace");
    std::fs::write(&outside, "host sentinel").expect("outside sentinel");
    let working_dir = working_dir.canonicalize().expect("canonical workspace");
    let ctx = ToolContext {
        working_dir: working_dir.clone(),
        sandbox_root: Some(working_dir.clone()),
        ..Default::default()
    };

    let wrapped =
        sandboxed_shell_command("printf safe > inside.txt", &ctx).expect("bubblewrap command");
    assert!(wrapped.starts_with("/usr/bin/bwrap "));

    let escaped = BashTool
        .execute(
            json!({"command": format!(
                "printf escaped > {}",
                shell_words::quote(&outside.to_string_lossy())
            )}),
            &ctx,
        )
        .await;
    // A TMPDIR under /tmp is hidden by private scratch, where this write may
    // succeed. The protected invariant is that the host file cannot change.
    assert_eq!(
        std::fs::read_to_string(&outside).expect("outside sentinel remains readable"),
        "host sentinel",
        "sandbox changed a host file outside its root: {}",
        escaped.output
    );

    let inside = BashTool
        .execute(
            json!({"command": format!(
                "test -d {0} && printf safe > {0}/inside.txt",
                shell_words::quote(&working_dir.to_string_lossy())
            )}),
            &ctx,
        )
        .await;
    assert!(!inside.is_error, "{}", inside.output);
    assert_eq!(
        std::fs::read_to_string(working_dir.join("inside.txt")).expect("inside output"),
        "safe"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn foreground_bash_never_claims_equal_directory_or_symlink_is_unchanged() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let state_dir = temp.path().join("state");
    std::fs::create_dir(&state_dir).expect("state dir");
    std::fs::create_dir(working_dir.join("existing")).expect("existing dir");
    std::fs::write(working_dir.join("target"), "value").expect("target");
    symlink("target", working_dir.join("link")).expect("symlink");
    let ctx = ToolContext {
        working_dir: working_dir.clone(),
        db_path: Some(state_dir.join("mitsuro.db")),
        ..Default::default()
    };

    let directory = BashTool
        .execute(json!({"command": "command mkdir -p ./existing"}), &ctx)
        .await;
    let symlink = BashTool
        .execute(json!({"command": "printf value > link"}), &ctx)
        .await;
    assert!(!directory.is_error, "{}", directory.output);
    assert!(!symlink.is_error, "{}", symlink.output);
    assert_eq!(changed_value(&directory), None);
    assert_eq!(changed_value(&symlink), None);
}

#[cfg(unix)]
#[tokio::test]
async fn truncated_output_keeps_recoverable_full_log() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let ctx = ToolContext {
        working_dir: working_dir.clone(),
        session_id: Some("test-session".to_string()),
        ..Default::default()
    };
    let command = "i=1; while [ $i -le 5000 ]; do printf 'line-%s\\n' \"$i\"; i=$((i+1)); done";

    let result = BashTool.execute(json!({"command": command}), &ctx).await;
    assert!(!result.is_error, "{}", result.output);

    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    let preview = envelope["data"]["output"].as_str().expect("output preview");
    assert!(preview.contains("Output truncated"));
    assert!(preview.contains("Full output saved to"));
    assert!(!preview.contains("line-1\n"));
    assert!(preview.contains("line-5000"));

    let directory = working_dir.join(".mitsuro/tool-output/test-session");
    let paths = std::fs::read_dir(directory)
        .expect("output directory")
        .map(|entry| entry.expect("output entry").path())
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1);
    let full = std::fs::read_to_string(&paths[0]).expect("full output log");
    assert!(full.contains("line-1\n"));
    assert!(full.contains("line-5000\n"));
}

#[cfg(unix)]
#[tokio::test]
async fn background_start_reports_fast_failure_with_process_handle_and_stderr() {
    let temp = tempfile::tempdir().expect("tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let ctx = ToolContext::with_process_registry(
        temp.path().canonicalize().expect("canonical tempdir"),
        registry,
    );

    let result = BashTool
        .execute(
            json!({
                "command": "echo 'address already in use' >&2; exit 7",
                "run_in_background": true
            }),
            &ctx,
        )
        .await;

    assert!(result.is_error, "{}", result.output);
    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    assert_eq!(
        envelope["error"]["code"].as_str(),
        Some("background_start_failed")
    );
    assert_eq!(envelope["data"]["status"].as_str(), Some("failed"));
    assert!(envelope["data"]["process_id"].is_string());
    assert!(envelope["data"]["error"]
        .as_str()
        .is_some_and(|error| error.contains("address already in use")));
}

#[cfg(unix)]
#[tokio::test]
async fn background_start_returns_trackable_running_process() {
    let temp = tempfile::tempdir().expect("tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let ctx = ToolContext::with_process_registry(
        temp.path().canonicalize().expect("canonical tempdir"),
        Arc::clone(&registry),
    );

    let result = BashTool
        .execute(
            json!({"command": "sleep 5", "run_in_background": true}),
            &ctx,
        )
        .await;

    assert!(!result.is_error, "{}", result.output);
    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    let process_id = envelope["data"]["process_id"].as_str().expect("process id");
    assert_eq!(envelope["data"]["status"].as_str(), Some("running"));
    assert!(envelope["data"]["next_action"]
        .as_str()
        .is_some_and(|guidance| guidance.contains("processes")));

    registry.kill(process_id).await.expect("kill test process");
}

#[cfg(unix)]
#[tokio::test]
async fn background_start_without_registry_refuses_untracked_process() {
    let temp = tempfile::tempdir().expect("tempdir");
    let ctx = ToolContext {
        working_dir: temp.path().canonicalize().expect("canonical tempdir"),
        ..Default::default()
    };

    let result = BashTool
        .execute(
            json!({
                "command": "sleep 30 # --host 127.0.0.1 --port 45941",
                "run_in_background": true
            }),
            &ctx,
        )
        .await;

    assert!(result.is_error, "{}", result.output);
    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    assert_eq!(
        envelope["error"]["code"].as_str(),
        Some("background_registry_unavailable")
    );
    assert_eq!(envelope["data"]["status"].as_str(), Some("not_started"));
}

#[cfg(unix)]
#[tokio::test]
async fn equivalent_background_launch_reuses_owner_scoped_process() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let owner_a = ToolContext::with_process_registry(working_dir.clone(), Arc::clone(&registry))
        .with_user_id("owner-a".to_string());
    let command = "sleep 30 # --host 127.0.0.1 --port 45940";

    let first = BashTool
        .execute(
            json!({"command": command, "run_in_background": true}),
            &owner_a,
        )
        .await;
    assert!(!first.is_error, "{}", first.output);
    let first_envelope: serde_json::Value =
        serde_json::from_str(&first.output).expect("first tool JSON");
    let process_id = first_envelope["data"]["process_id"]
        .as_str()
        .expect("first process id")
        .to_string();
    assert_eq!(
        first_envelope["data"]["endpoint_hints"],
        json!(["127.0.0.1:45940"])
    );

    let repeated_command = format!("cd {} && {command}", working_dir.display());
    let repeated = BashTool
        .execute(
            json!({"command": repeated_command, "run_in_background": true}),
            &owner_a,
        )
        .await;
    assert!(!repeated.is_error, "{}", repeated.output);
    let repeated_envelope: serde_json::Value =
        serde_json::from_str(&repeated.output).expect("repeated tool JSON");
    assert_eq!(
        repeated_envelope["data"]["process_id"].as_str(),
        Some(process_id.as_str())
    );
    assert_eq!(
        repeated_envelope["data"]["reused_existing"].as_bool(),
        Some(true)
    );
    assert_eq!(registry.list_for_user("owner-a").await.len(), 1);

    let owner_b = ToolContext::with_process_registry(working_dir.clone(), Arc::clone(&registry))
        .with_user_id("owner-b".to_string());
    let other_owner = BashTool
        .execute(
            json!({"command": command, "run_in_background": true}),
            &owner_b,
        )
        .await;
    assert!(!other_owner.is_error, "{}", other_owner.output);
    let other_envelope: serde_json::Value =
        serde_json::from_str(&other_owner.output).expect("other-owner tool JSON");
    let other_process_id = other_envelope["data"]["process_id"]
        .as_str()
        .expect("other-owner process id")
        .to_string();
    assert_ne!(other_process_id, process_id);
    assert_eq!(registry.list_for_user("owner-b").await.len(), 1);

    let default_owner = ToolContext::with_process_registry(working_dir, Arc::clone(&registry));
    let unscoped = BashTool
        .execute(
            json!({"command": command, "run_in_background": true}),
            &default_owner,
        )
        .await;
    assert!(!unscoped.is_error, "{}", unscoped.output);
    let unscoped_envelope: serde_json::Value =
        serde_json::from_str(&unscoped.output).expect("default-owner tool JSON");
    let unscoped_process_id = unscoped_envelope["data"]["process_id"]
        .as_str()
        .expect("default-owner process id")
        .to_string();
    assert_ne!(unscoped_process_id, process_id);
    assert_ne!(unscoped_process_id, other_process_id);
    assert_eq!(registry.list().await.len(), 1);

    registry
        .kill_for_user("owner-a", &process_id)
        .await
        .expect("kill owner-a process");
    registry
        .kill_for_user("owner-b", &other_process_id)
        .await
        .expect("kill owner-b process");
    registry
        .kill(&unscoped_process_id)
        .await
        .expect("kill default-owner process");
}

#[cfg(unix)]
#[tokio::test]
async fn exact_non_endpoint_background_launch_is_reused_and_not_recredited() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let ctx = ToolContext::with_process_registry(working_dir, Arc::clone(&registry))
        .with_user_id("plain-background-owner".to_string());

    let first = BashTool
        .execute(
            json!({"command": "sleep 30", "run_in_background": true}),
            &ctx,
        )
        .await;
    let repeated = BashTool
        .execute(
            json!({"command": "sleep 30", "run_in_background": true}),
            &ctx,
        )
        .await;
    assert!(!first.is_error, "{}", first.output);
    assert!(!repeated.is_error, "{}", repeated.output);
    assert_eq!(changed_value(&first), Some(true));
    assert_eq!(changed_value(&repeated), Some(false));

    let first_envelope: serde_json::Value =
        serde_json::from_str(&first.output).expect("first tool JSON");
    let repeated_envelope: serde_json::Value =
        serde_json::from_str(&repeated.output).expect("repeated tool JSON");
    assert_eq!(
        first_envelope["data"]["process_id"],
        repeated_envelope["data"]["process_id"]
    );
    assert_eq!(repeated_envelope["data"]["reused_existing"], json!(true));
    assert_eq!(
        registry.list_for_user("plain-background-owner").await.len(),
        1
    );

    registry
        .kill_for_user(
            "plain-background-owner",
            first_envelope["data"]["process_id"]
                .as_str()
                .expect("process id"),
        )
        .await
        .expect("kill plain background process");
}

#[cfg(unix)]
#[tokio::test]
async fn delegated_background_launch_uses_process_owner_without_changing_tenant_owner() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let mut ctx = ToolContext::with_process_registry(working_dir, Arc::clone(&registry))
        .with_user_id("tenant-a".to_string())
        .with_process_owner_id("tenant-a:hive:task-a".to_string());
    ctx.session_id = Some("parent-session".to_string());

    let result = BashTool
        .execute(
            json!({"command": "sleep 30", "run_in_background": true}),
            &ctx,
        )
        .await;
    assert!(!result.is_error, "{}", result.output);
    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    let process_id = envelope["data"]["process_id"].as_str().expect("process id");

    assert!(registry.list_for_user("tenant-a").await.is_empty());
    let process = registry
        .get_for_user("tenant-a:hive:task-a", process_id)
        .await
        .expect("task-scoped process");
    assert_eq!(process.session_id, None);
    registry
        .kill_for_user("tenant-a:hive:task-a", process_id)
        .await
        .expect("cleanup process");
}

#[cfg(unix)]
#[tokio::test]
async fn sanitized_background_launch_hides_sensitive_values_and_does_not_reuse_inherited_job() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let isolated_home = working_dir.join("isolated-home");
    std::fs::create_dir_all(&isolated_home).expect("isolated home");
    let registry = Arc::new(ProcessRegistry::new());
    let owner = "environment-policy-owner";
    let command = "printf '%s|%s\\n' \"${MITSURO_TEST_SECRET-unset}\" \"$HOME\"; sleep 30";
    let command_environment = std::collections::BTreeMap::from([
        ("HOME".to_string(), isolated_home.display().to_string()),
        (
            "MITSURO_TEST_SECRET".to_string(),
            "must-not-escape".to_string(),
        ),
    ]);

    let mut sanitized =
        ToolContext::with_process_registry(working_dir.clone(), Arc::clone(&registry))
            .with_user_id(owner.to_string());
    sanitized.command_environment = command_environment.clone();
    sanitized.command_environment_policy = CommandEnvironmentPolicy::Sanitized;
    let sanitized_result = BashTool
        .execute(
            json!({"command": command, "run_in_background": true}),
            &sanitized,
        )
        .await;
    assert!(!sanitized_result.is_error, "{}", sanitized_result.output);
    let sanitized_envelope: serde_json::Value =
        serde_json::from_str(&sanitized_result.output).expect("sanitized tool JSON");
    let sanitized_id = sanitized_envelope["data"]["process_id"]
        .as_str()
        .expect("sanitized process id")
        .to_string();
    let (output, _) = registry
        .output_for_user(owner, &sanitized_id)
        .await
        .expect("sanitized output");
    assert_eq!(output.trim(), format!("unset|{}", isolated_home.display()));

    let mut inherited = ToolContext::with_process_registry(working_dir, Arc::clone(&registry))
        .with_user_id(owner.to_string());
    inherited.command_environment = command_environment;
    let inherited_result = BashTool
        .execute(
            json!({"command": command, "run_in_background": true}),
            &inherited,
        )
        .await;
    assert!(!inherited_result.is_error, "{}", inherited_result.output);
    let inherited_envelope: serde_json::Value =
        serde_json::from_str(&inherited_result.output).expect("inherited tool JSON");
    let inherited_id = inherited_envelope["data"]["process_id"]
        .as_str()
        .expect("inherited process id")
        .to_string();

    assert_ne!(sanitized_id, inherited_id);
    assert_eq!(registry.list_for_user(owner).await.len(), 2);

    registry
        .kill_for_user(owner, &sanitized_id)
        .await
        .expect("kill sanitized process");
    registry
        .kill_for_user(owner, &inherited_id)
        .await
        .expect("kill inherited process");
}

#[cfg(unix)]
#[tokio::test]
async fn detached_shell_wrapper_is_canonicalized_and_reused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let ctx = ToolContext::with_process_registry(working_dir, Arc::clone(&registry))
        .with_user_id("detachment-owner".to_string());
    let canonical = "sh -c 'sleep 30' preview --host 127.0.0.1 --port 45941";
    let wrapped = format!("nohup {canonical} > /dev/null 2>&1 &");

    let first = BashTool
        .execute(json!({"command": wrapped, "run_in_background": true}), &ctx)
        .await;
    assert!(!first.is_error, "{}", first.output);
    let first_envelope: serde_json::Value =
        serde_json::from_str(&first.output).expect("first tool JSON");
    let process_id = first_envelope["data"]["process_id"]
        .as_str()
        .expect("first process id")
        .to_string();
    assert!(first_envelope["warnings"]
        .as_array()
        .expect("warnings")
        .iter()
        .any(|warning| warning
            .as_str()
            .is_some_and(|text| text.contains("process registry"))));

    let tracked = registry
        .get_for_user("detachment-owner", &process_id)
        .await
        .expect("tracked process");
    assert_eq!(tracked.command, canonical);

    let repeated = BashTool
        .execute(
            json!({"command": canonical, "run_in_background": true}),
            &ctx,
        )
        .await;
    assert!(!repeated.is_error, "{}", repeated.output);
    let repeated_envelope: serde_json::Value =
        serde_json::from_str(&repeated.output).expect("repeated tool JSON");
    assert_eq!(
        repeated_envelope["data"]["process_id"].as_str(),
        Some(process_id.as_str())
    );
    assert_eq!(
        repeated_envelope["data"]["reused_existing"].as_bool(),
        Some(true)
    );

    let processes = registry.list_for_user("detachment-owner").await;
    assert_eq!(processes.len(), 1);
    assert!(processes[0].is_active());

    registry
        .kill_for_user("detachment-owner", &process_id)
        .await
        .expect("kill detachment test process");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_equivalent_background_launches_spawn_exactly_once() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let command =
        "printf 'started\\n' >> concurrent-launches.txt; sleep 30 # --host 127.0.0.1 --port 45942";

    let launch = |barrier: Arc<tokio::sync::Barrier>, registry: Arc<ProcessRegistry>| {
        let working_dir = working_dir.clone();
        async move {
            let ctx = ToolContext::with_process_registry(working_dir, registry)
                .with_user_id("concurrent-owner".to_string());
            barrier.wait().await;
            BashTool
                .execute(json!({"command": command, "run_in_background": true}), &ctx)
                .await
        }
    };

    let first = tokio::spawn(launch(Arc::clone(&barrier), Arc::clone(&registry)));
    let second = tokio::spawn(launch(barrier, Arc::clone(&registry)));
    let (first, second) = tokio::join!(first, second);
    let results = [
        first.expect("first concurrent launch task"),
        second.expect("second concurrent launch task"),
    ];
    assert!(results.iter().all(|result| !result.is_error));

    let envelopes = results
        .iter()
        .map(|result| serde_json::from_str::<serde_json::Value>(&result.output).expect("tool JSON"))
        .collect::<Vec<_>>();
    let process_ids = envelopes
        .iter()
        .map(|envelope| envelope["data"]["process_id"].as_str().expect("process id"))
        .collect::<Vec<_>>();
    assert_eq!(process_ids[0], process_ids[1]);
    assert_eq!(
        envelopes
            .iter()
            .filter(|envelope| envelope["data"]["reused_existing"] == json!(true))
            .count(),
        1
    );
    assert_eq!(registry.list_for_user("concurrent-owner").await.len(), 1);
    assert_eq!(
        std::fs::read_to_string(working_dir.join("concurrent-launches.txt"))
            .expect("launch marker")
            .lines()
            .count(),
        1,
        "only one OS process should have crossed the atomic launch boundary"
    );

    registry
        .kill_for_user("concurrent-owner", process_ids[0])
        .await
        .expect("kill concurrent test process");
}

#[cfg(unix)]
#[tokio::test]
async fn different_background_server_cannot_reuse_an_owned_endpoint() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let registry = Arc::new(ProcessRegistry::new());
    let ctx = ToolContext::with_process_registry(working_dir, Arc::clone(&registry))
        .with_user_id("endpoint-owner".to_string());

    let first = BashTool
        .execute(
            json!({
                "command": "sleep 30 # --host 127.0.0.1 --port 45943",
                "run_in_background": true
            }),
            &ctx,
        )
        .await;
    assert!(!first.is_error, "{}", first.output);

    let second = BashTool
        .execute(
            json!({
                "command": "sleep 29 # --host 127.0.0.1 --port 45943",
                "run_in_background": true
            }),
            &ctx,
        )
        .await;
    assert!(second.is_error, "{}", second.output);
    let envelope: serde_json::Value = serde_json::from_str(&second.output).expect("tool JSON");
    assert_eq!(envelope["error"]["code"], "background_endpoint_in_use");

    let first_envelope: serde_json::Value =
        serde_json::from_str(&first.output).expect("first tool JSON");
    registry
        .kill_for_user(
            "endpoint-owner",
            first_envelope["data"]["process_id"]
                .as_str()
                .expect("process id"),
        )
        .await
        .expect("kill first server");
}

#[cfg(unix)]
#[tokio::test]
async fn timeout_kills_entire_foreground_process_group() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let pid_file_name = "timeout-process-tree.pids";
    let pid_file = working_dir.join(pid_file_name);
    let command = foreground_process_tree_command(&working_dir, pid_file_name);

    let result = execute_foreground(
        command,
        Duration::from_millis(750),
        None,
        working_dir.join("timeout-output.log"),
    )
    .await;
    let (leader_pid, descendant_pid) = wait_for_pid_file(&pid_file).await;
    let mut cleanup = TestProcessCleanup::new(vec![leader_pid, descendant_pid]);

    assert!(result.is_error, "{}", result.output);
    let envelope: serde_json::Value = serde_json::from_str(&result.output).expect("tool JSON");
    assert_eq!(envelope["error"]["code"].as_str(), Some("timeout"));
    let pids = [leader_pid, descendant_pid];
    let exited = wait_for_processes_to_exit(&pids).await;
    if exited {
        cleanup.disarm();
    }
    assert!(
        exited,
        "timeout left foreground process tree alive: leader={leader_pid}, descendant={descendant_pid}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_foreground_execution_kills_entire_process_group() {
    let temp = tempfile::tempdir().expect("tempdir");
    let working_dir = temp.path().canonicalize().expect("canonical tempdir");
    let pid_file_name = "cancel-process-tree.pids";
    let pid_file = working_dir.join(pid_file_name);
    let command = foreground_process_tree_command(&working_dir, pid_file_name);

    let execution = tokio::spawn(execute_foreground(
        command,
        Duration::from_secs(60),
        None,
        working_dir.join("cancel-output.log"),
    ));
    let (leader_pid, descendant_pid) = wait_for_pid_file(&pid_file).await;
    let mut cleanup = TestProcessCleanup::new(vec![leader_pid, descendant_pid]);
    assert!(process_is_alive(leader_pid));
    assert!(process_is_alive(descendant_pid));

    execution.abort();
    let join_error = execution
        .await
        .expect_err("aborted foreground execution should be cancelled");
    assert!(join_error.is_cancelled());

    let pids = [leader_pid, descendant_pid];
    let exited = wait_for_processes_to_exit(&pids).await;
    if exited {
        cleanup.disarm();
    }
    assert!(
        exited,
        "dropping foreground execution left process tree alive: leader={leader_pid}, descendant={descendant_pid}"
    );
}
