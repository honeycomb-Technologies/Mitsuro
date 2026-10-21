//! Agent Skills-compatible progressive-disclosure system.
//!
//! Skills are modular, filesystem-based resources that provide Claude with
//! domain-specific expertise: workflows, context, and best practices.
//!
//! # Directory Structure
//!
//! Native Mitsuro roots are `~/.mitsuro/skills/` and `.mitsuro/skills/`. The
//! manager also discovers `.agents`, OpenCode, Claude, Codex, Pi, and registered
//! package roots with deterministic precedence.
//!
//! Each skill is a directory containing a `SKILL.md` file with YAML frontmatter:
//!
//! ```yaml
//! ---
//! name: skill-name
//! description: Brief description for discovery
//! version: 1.0.0
//! ---
//!
//! # Skill Name
//!
//! [Instructions...]
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use mitsuro_core::skills::SkillsManager;
//!
//! let mut manager = SkillsManager::with_defaults(&working_dir);
//!
//! // List available skills
//! for skill in manager.list_skills() {
//!     println!("{}: {}", skill.name, skill.description);
//! }
//!
//! // Load skill content for AI context
//! let content = manager.load_skill_content("git-commit")?;
//! ```

mod loader;
mod manager;
mod skill;

pub use manager::{SkillRoot, SkillsManager};
pub use skill::{Skill, SkillInfo, SkillPermission, SkillSource};

// Re-export loader functions for direct use
pub use loader::{
    load_skill, load_skill_file, load_skills_from_dir, load_skills_from_root, SkillDiagnostic,
    SkillDiagnosticSeverity, SkillLoadOptions, SkillLoadReport,
};
