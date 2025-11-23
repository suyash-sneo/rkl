use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;

use crate::paths;
use crate::query::ast::{OrderDir, OrderField};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(alias = "timestamp_limit_multiplier")]
    pub query_scan_multiplier: usize,
    pub default_order_field: DefaultOrderField,
    pub default_order_dir: DefaultOrderDir,
    pub default_limit: Option<usize>,
    pub default_timestamps_use_utc: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefaultOrderField {
    Timestamp,
    Poffset,
    PoffsetTs,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DefaultOrderDir {
    Asc,
    Desc,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            query_scan_multiplier: 5,
            default_order_field: DefaultOrderField::Poffset,
            default_order_dir: DefaultOrderDir::Desc,
            default_limit: None,
            default_timestamps_use_utc: true,
        }
    }
}

impl AppConfig {
    pub fn load() -> Self {
        let path = paths::app_config_path();
        if let Ok(raw) = fs::read_to_string(&path) {
            if let Ok(cfg) = serde_json::from_str::<AppConfig>(&raw) {
                return cfg;
            }
        }
        AppConfig::default()
    }

    pub fn save(&self) -> Result<()> {
        let path = paths::app_config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let serialized = serde_json::to_string_pretty(self)?;
        fs::write(path, serialized)?;
        Ok(())
    }

    pub fn default_order(&self) -> (OrderField, OrderDir) {
        let field = match self.default_order_field {
            DefaultOrderField::Timestamp => OrderField::Timestamp,
            DefaultOrderField::Poffset => OrderField::Poffset,
            DefaultOrderField::PoffsetTs => OrderField::PoffsetTs,
        };
        let dir = match self.default_order_dir {
            DefaultOrderDir::Asc => OrderDir::Asc,
            DefaultOrderDir::Desc => OrderDir::Desc,
        };
        (field, dir)
    }
}
