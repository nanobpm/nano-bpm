//! Build-time docs processor.
//!
//! The User Guide served at `/guide` is **single-sourced** from `USERGUIDE.md`
//! (the end-user / operator manual — deliberately separate from the contributor
//! `README.md`, which is source-code focused). At build time we chunk the guide
//! on its H2 (`##`) headings into one page per section — the preamble before the
//! first H2 becomes the "Overview" home page — render each section's Markdown to
//! HTML (heading ids + rewritten links), and emit a small generated Rust file
//! that `main.rs` embeds with `include!`. So the guide ships inside the binary,
//! works fully offline, and never drifts from the manual.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

use pulldown_cmark::{html, Event, Options, Parser, Tag, TagEnd};

/// Repo-relative README links are rewritten to GitHub blob URLs so they resolve
/// from the shipped, standalone guide.
const REPO_BLOB: &str = "https://github.com/nanobpm/nano-bpm/blob/main/processos/";

/// H2 sections that belong in the contributor-facing README but NOT in the
/// end-user guide (matched case-insensitively against the heading text).
const EXCLUDE: &[&str] = &["layout"];

struct Section {
    heading: Option<String>,
    lines: Vec<String>,
}

struct Page {
    slug: String,
    title: String,
    markdown: String,
}

/// Resolve the version the built binary reports. Release CI exports
/// `PROCESSOS_VERSION` from the pushed tag (the single source of truth for a
/// release); locally we fall back to `git describe`, then the crate version in
/// `Cargo.toml`. A leading `v` is stripped so `v1.2.3` -> `1.2.3`.
fn resolve_version(manifest: &str) -> String {
    if let Ok(v) = env::var("PROCESSOS_VERSION") {
        let v = v.trim().trim_start_matches('v');
        if !v.is_empty() {
            return v.to_string();
        }
    }
    let git = std::process::Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty"])
        .current_dir(manifest)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .trim_start_matches('v')
                .to_string()
        })
        .filter(|v| !v.is_empty());
    if let Some(v) = git {
        return v;
    }
    env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string())
}

#[allow(
    clippy::expect_used,
    reason = "build script: a missing cargo env var or an unreadable USERGUIDE.md is a fatal build misconfiguration — panicking fails the build loudly, which is the desired behaviour here"
)]
fn main() {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");

    // Stamp the reported version (see `resolve_version`). Declared as an env
    // dependency so a changed tag forces build.rs to rerun in CI.
    println!("cargo:rerun-if-env-changed=PROCESSOS_VERSION");
    let version = resolve_version(&manifest);
    println!("cargo:rustc-env=PROCESSOS_VERSION={version}");

    let readme_path = Path::new(&manifest).join("USERGUIDE.md");
    println!("cargo:rerun-if-changed=USERGUIDE.md");

    let markdown = fs::read_to_string(&readme_path).expect("read USERGUIDE.md");

    // --- 1. Chunk the README into sections on H2 boundaries (skip fenced code).
    let mut sections: Vec<Section> = Vec::new();
    let mut current = Section {
        heading: None,
        lines: Vec::new(),
    };
    let mut in_fence = false;
    for line in markdown.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            in_fence = !in_fence;
        }
        if !in_fence {
            if let Some(h) = line.strip_prefix("## ") {
                if !current.lines.is_empty() || current.heading.is_some() {
                    sections.push(current);
                }
                current = Section {
                    heading: Some(h.trim().to_string()),
                    lines: Vec::new(),
                };
                continue;
            }
        }
        current.lines.push(line.to_string());
    }
    if !current.lines.is_empty() || current.heading.is_some() {
        sections.push(current);
    }

    // Drop contributor-only sections from the shipped guide.
    let visible: Vec<Section> = sections
        .into_iter()
        .filter(|s| {
            s.heading
                .as_deref()
                .map(|h| !EXCLUDE.contains(&h.to_ascii_lowercase().as_str()))
                .unwrap_or(true)
        })
        .collect();

    // --- 2. Build the page list: preamble -> Overview, then one page per H2.
    let mut iter = visible.into_iter();
    let preamble = iter.next().unwrap_or(Section {
        heading: None,
        lines: Vec::new(),
    });
    let preamble_md = preamble.lines.join("\n");
    let overview_title = first_h1(&preamble_md).unwrap_or_else(|| "Overview".to_string());

    let mut pages: Vec<Page> = Vec::new();
    pages.push(Page {
        slug: "index".to_string(),
        title: "Overview".to_string(),
        // Keep the H1 in the body so the Overview page renders a real title.
        markdown: preamble_md,
    });
    let _ = overview_title; // (kept for clarity; the H1 renders inline)
    for s in iter {
        let heading = s.heading.unwrap_or_default();
        let slug = slugify(&heading);
        // Re-prepend the H2 so it renders (with an id) at the top of its page.
        let body_md = format!("## {heading}\n{}", s.lines.join("\n"));
        pages.push(Page {
            slug,
            title: heading,
            markdown: body_md,
        });
    }

    // --- 3. Map every heading slug -> the page that owns it, so intra-document
    // anchor links (`#some-heading`) target the right page.
    let mut anchor_to_page: HashMap<String, String> = HashMap::new();
    for p in &pages {
        let mut fence = false;
        for line in p.markdown.lines() {
            let t = line.trim_start();
            if t.starts_with("```") {
                fence = !fence;
                continue;
            }
            if fence {
                continue;
            }
            if let Some(rest) = heading_text(line) {
                anchor_to_page.insert(slugify(&rest), p.slug.clone());
            }
        }
    }

    // --- 4. Render each page's Markdown to HTML.
    let mut generated = String::new();
    generated.push_str("// @generated by build.rs from USERGUIDE.md — do not edit.\n");
    generated.push_str("pub static USERGUIDE_PAGES: &[GuidePage] = &[\n");
    for p in &pages {
        let body = render_markdown(&p.markdown, &anchor_to_page);
        generated.push_str(&format!(
            "    GuidePage {{ slug: {:?}, title: {:?}, body: {:?} }},\n",
            p.slug, p.title, body
        ));
    }
    generated.push_str("];\n");

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR");
    let out_path = Path::new(&out_dir).join("userguide.rs");
    fs::write(&out_path, generated).expect("write userguide.rs");
}

/// First `# H1` text in a Markdown blob.
fn first_h1(md: &str) -> Option<String> {
    md.lines()
        .find_map(|l| l.strip_prefix("# ").map(|s| s.trim().to_string()))
}

/// Heading text for any `#`..`######` line (else `None`).
fn heading_text(line: &str) -> Option<String> {
    let t = line.trim_start();
    if !t.starts_with('#') {
        return None;
    }
    let rest = t.trim_start_matches('#');
    if rest.starts_with(' ') {
        Some(rest.trim().to_string())
    } else {
        None
    }
}

/// GitHub-compatible heading slug (matches the README's own intra-doc anchors).
fn slugify(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let lower = text.to_lowercase();
    // Strip inline code backticks and any HTML tags first.
    let mut in_tag = false;
    for ch in lower.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if in_tag => {}
            '`' => {}
            c if c.is_alphanumeric() || c == '-' || c == '_' => out.push(c),
            ' ' => out.push(' '),
            _ => {}
        }
    }
    out.trim().replace(' ', "-")
}

/// Render a Markdown section to HTML: inject GitHub-style ids onto headings (so
/// in-page anchors resolve) and rewrite link targets for the standalone,
/// multi-page guide site.
fn render_markdown(md: &str, anchor_to_page: &HashMap<String, String>) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    let mut events: Vec<Event> = Parser::new_ext(md, opts).collect();

    // Inject heading ids derived from the heading's own text.
    let mut i = 0;
    while i < events.len() {
        let replacement = if let Event::Start(Tag::Heading {
            level,
            id,
            classes,
            attrs,
        }) = &events[i]
        {
            if id.is_none() {
                let mut text = String::new();
                let mut j = i + 1;
                while j < events.len() {
                    match &events[j] {
                        Event::Text(t) | Event::Code(t) => text.push_str(t),
                        Event::End(TagEnd::Heading(_)) => break,
                        _ => {}
                    }
                    j += 1;
                }
                let slug = slugify(&text);
                Some(Event::Start(Tag::Heading {
                    level: *level,
                    id: Some(slug.into()),
                    classes: classes.clone(),
                    attrs: attrs.clone(),
                }))
            } else {
                None
            }
        } else {
            None
        };
        if let Some(r) = replacement {
            events[i] = r;
        }
        i += 1;
    }

    // Rewrite link destinations.
    for ev in events.iter_mut() {
        if let Event::Start(Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }) = ev
        {
            let new = rewrite_href(dest_url, anchor_to_page);
            *ev = Event::Start(Tag::Link {
                link_type: *link_type,
                dest_url: new.into(),
                title: title.clone(),
                id: id.clone(),
            });
        }
    }

    let mut out = String::new();
    html::push_html(&mut out, events.into_iter());
    out
}

fn rewrite_href(href: &str, anchor_to_page: &HashMap<String, String>) -> String {
    if href.is_empty() {
        return href.to_string();
    }
    let lower = href.to_ascii_lowercase();
    if lower.starts_with("http:") || lower.starts_with("https:") || lower.starts_with("mailto:") {
        return href.to_string();
    }
    if let Some(anchor) = href.strip_prefix('#') {
        if let Some(page) = anchor_to_page.get(anchor) {
            return if page == "index" {
                format!("/guide#{anchor}")
            } else {
                format!("/guide/{page}#{anchor}")
            };
        }
        return href.to_string(); // unknown anchor — leave as-is
    }
    // Repo-relative path (optionally with its own #fragment) -> GitHub blob URL.
    let (path, frag) = match href.split_once('#') {
        Some((p, f)) => (p, Some(f)),
        None => (href, None),
    };
    let path = path.trim_start_matches("./");
    let mut url = format!("{REPO_BLOB}{path}");
    if let Some(f) = frag {
        url.push('#');
        url.push_str(f);
    }
    url
}
