mod agent;
mod app;
mod browser_surface;
mod preferences;
mod resource_journal;
mod resources;
mod server_supervisor;
mod spatial;
mod terminal_surface;
mod theme;

fn main() {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let first = arguments.next();
    if arguments.next().is_none()
        && first.as_deref().is_some_and(|argument| {
            argument == std::ffi::OsStr::new("--version") || argument == std::ffi::OsStr::new("-V")
        })
    {
        println!("mitsuro-canvas {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    app::run();
}
