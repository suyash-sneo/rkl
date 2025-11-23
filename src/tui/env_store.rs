use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use super::pem_utils::encode_pem_for_storage;
use crate::paths;

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
        let mut envs: Vec<Environment> = Vec::new();
        let mut seen = HashSet::new();
        for dir in [paths::envs_dir_new(), paths::envs_dir_legacy()] {
            if let Ok(entries) = fs::read_dir(&dir) {
                for ent in entries.flatten() {
                    let path = ent.path();
                    if path.is_file() {
                        if let Some(ext) = path.extension() {
                            if ext != "json" {
                                continue;
                            }
                        }
                        if let Ok(s) = fs::read_to_string(&path) {
                            if let Ok(e) = serde_json::from_str::<Environment>(&s) {
                                if seen.insert(e.name.clone()) {
                                    envs.push(e);
                                }
                            }
                        }
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
            let mut e_enc = e.clone();
            e_enc.private_key_pem = e_enc
                .private_key_pem
                .as_ref()
                .map(|s| encode_pem_for_storage(s));
            e_enc.public_key_pem = e_enc
                .public_key_pem
                .as_ref()
                .map(|s| encode_pem_for_storage(s));
            e_enc.ssl_ca_pem = e_enc.ssl_ca_pem.as_ref().map(|s| encode_pem_for_storage(s));
            let s = serde_json::to_string_pretty(&e_enc).context("serialize env")?;
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
    paths::envs_dir_new()
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if is_safe(c) { c } else { '_' })
        .collect()
}
fn is_safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'
}
