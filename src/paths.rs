use std::path::PathBuf;

/// Return the root configuration directory for rkl, defaulting to `.rkl` when
/// `$HOME` is not set.
pub fn rkl_home_dir() -> PathBuf {
    match std::env::var("HOME") {
        Ok(home) => PathBuf::from(home).join(".rkl"),
        Err(_) => PathBuf::from(".rkl"),
    }
}

pub fn envs_dir_new() -> PathBuf {
    rkl_home_dir().join("configs").join("envs")
}

pub fn envs_dir_legacy() -> PathBuf {
    rkl_home_dir().join("envs")
}

pub fn app_config_path() -> PathBuf {
    rkl_home_dir().join("configs").join("app-config.json")
}

pub fn history_dir() -> PathBuf {
    rkl_home_dir().join("history")
}

pub fn history_file_path() -> PathBuf {
    history_dir().join("query-history.txt")
}

pub fn legacy_history_file_path() -> PathBuf {
    envs_dir_legacy().join("query-history.txt")
}

pub fn logs_dir() -> PathBuf {
    rkl_home_dir().join("logs")
}
