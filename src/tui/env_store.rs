use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Environment {
    pub name: String,
    pub host: String,
    pub private_key_pem: Option<String>,
    pub public_key_pem: Option<String>,
    pub ssl_ca_pem: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EnvStore {
    pub envs: Vec<Environment>,
    pub selected: Option<usize>,
}

impl EnvStore {
    pub fn load() -> Self {
        let dir = config_dir();
        let mut envs: Vec<Environment> = Vec::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for ent in entries.flatten() {
                let path = ent.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() { if ext != "json" { continue; } }
                    if let Ok(s) = fs::read_to_string(&path) {
                        if let Ok(e) = serde_json::from_str::<Environment>(&s) { envs.push(e); }
                    }
                }
            }
        }
        envs.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        let selected = if envs.is_empty() { None } else { Some(0) };
        Self { envs, selected }
    }
    pub fn save(&self) -> Result<()> {
        let dir = config_dir();
        fs::create_dir_all(&dir).context("create env dir")?;
        // track desired files
        let mut desired: HashSet<String> = HashSet::new();
        for e in &self.envs {
            let fname = format!("{}.json", sanitize(&e.name));
            desired.insert(fname.clone());
            let path = dir.join(fname);
            let s = serde_json::to_string_pretty(e).context("serialize env")?;
            fs::write(path, s).context("write env file")?;
        }
        // remove stale
        if let Ok(entries) = fs::read_dir(&dir) {
            for ent in entries.flatten() {
                let path = ent.path();
                if path.is_file() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".json") && !desired.contains(name) {
                            let _ = fs::remove_file(path);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn config_dir() -> PathBuf {
    std::env::var("HOME").map(|h| PathBuf::from(h).join(".rkl").join("envs")).unwrap_or_else(|_| PathBuf::from(".rkl").join("envs"))
}

fn sanitize(name: &str) -> String { name.chars().map(|c| if is_safe(c) { c } else { '_' }).collect() }
fn is_safe(c: char) -> bool { c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' }
