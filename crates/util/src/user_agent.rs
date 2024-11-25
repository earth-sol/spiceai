/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::collections::HashMap;
use std::fmt;
/**
 * `SpiceUserAgent` represents a Spice user agent.
 * 
 * The Spice user agent is a string that identifies the client making a request to Spice
 * 
 * The Spice user agent string has the following format:
 * `<client_name>/<client_version> (<client_system>) <platform_name>/<platform_version> (<platform_system>) <extension_key>/<extension_value>`
 */
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SpiceUserAgent {
    pub client_name: String,
    pub client_version: String,
    pub client_system: Option<String>,
    pub platform_name: Option<String>,
    pub platform_version: Option<String>,
    pub platform_system: Option<String>,
    pub extensions: HashMap<String, String>,
}

impl SpiceUserAgent {
    #[must_use]
    pub fn with_client_name(mut self, client_name: &str) -> Self {
        self.client_name = client_name.to_string();
        self
    }

    #[must_use]
    pub fn with_client_version(mut self, client_version: &str) -> Self {
        self.client_version = client_version.to_string();
        self
    }

    #[must_use]
    pub fn with_client_version_from_cargo(mut self) -> Self {
        self.client_version = env!("CARGO_PKG_VERSION").to_string();
        self
    }

    #[must_use]
    pub fn with_client_system(mut self, client_system: &str) -> Self {
        self.client_system = Some(client_system.to_string());
        self
    }

    #[must_use]
    #[allow(clippy::assigning_clones)]
    pub fn with_client_system_option(mut self, client_system: &Option<String>) -> Self {
        self.client_system = client_system.clone();
        self
    }

    #[must_use]
    pub fn with_platform_name(mut self, platform_name: &str) -> Self {
        self.platform_name = Some(platform_name.to_string());
        self
    }

    #[must_use]
    #[allow(clippy::assigning_clones)]
    pub fn with_platform_name_option(mut self, platform_name: &Option<String>) -> Self {
        self.platform_name = platform_name.clone();
        self
    }

    #[must_use]
    pub fn with_platform_version(mut self, platform_version: &str) -> Self {
        self.platform_version = Some(platform_version.to_string());
        self
    }

    #[must_use]
    #[allow(clippy::assigning_clones)]
    pub fn with_platform_version_option(mut self, platform_version: &Option<String>) -> Self {
        self.platform_version = platform_version.clone();
        self
    }

    #[must_use]
    pub fn with_platform_system(mut self, platform_system: &str) -> Self {
        self.platform_system = Some(platform_system.to_string());
        self
    }

    #[must_use]
    #[allow(clippy::assigning_clones)]
    pub fn with_platform_system_option(mut self, platform_system: &Option<String>) -> Self {
        self.platform_system = platform_system.clone();
        self
    }

    #[must_use]
    pub fn with_extensions(mut self, extensions: HashMap<String, String>) -> Self {
        self.extensions = extensions;
        self
    }

    #[must_use]
    pub fn with_extension(mut self, key: &str, value: &str) -> Self {
        self.extensions.insert(key.to_string(), value.to_string());
        self
    }

    #[must_use]
    pub fn merge_client(&self, other: &SpiceUserAgent) -> SpiceUserAgent {
        let mut exts = self.extensions.clone();
        exts.extend(other.extensions.clone());

        SpiceUserAgent::default()
            .with_client_name(&other.client_name)
            .with_client_version(&other.client_version)
            .with_client_system_option(&other.client_system)
            .with_platform_name_option(&self.platform_name)
            .with_platform_version_option(&self.platform_version)
            .with_platform_system_option(&self.platform_system)
            .with_extensions(exts)
    }
}

impl Default for SpiceUserAgent {
    fn default() -> Self {
        Self {
            client_name: "unknown".to_string(),
            client_version: "unknown".to_string(),
            client_system: None,
            platform_name: Some("spiced".to_string()),
            platform_version: Some(get_platform_version()),
            platform_system: Some(get_platform_os_string()),
            extensions: HashMap::new(),
        }
    }
}

impl fmt::Display for SpiceUserAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}",
            self.client_name,
            self.client_version,
        )?;
        if let Some(client_system) = &self.client_system {
            write!(f, " ({client_system})")?;
        }
        if let (Some(platform_name), Some(platform_version)) = (&self.platform_name, &self.platform_version) {
            write!(f, " {platform_name}/{platform_version}")?;
            if let Some(platform_system) = &self.platform_system {
                write!(f, " ({platform_system})")?;
            }
        }
        for (key, value) in &self.extensions {
            write!(f, " {key}/{value}")?;
        }
        
        Ok(())
    }
}

impl TryFrom<String> for SpiceUserAgent {
    type Error = GenericError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        tracing::trace!("Parsing SpiceUserAgent from string: {}", s);
        let mut parts = s.split_whitespace();
        
        // Parse client info
        let client = parts.next().ok_or("Missing client info")?;
        let mut client_parts = client.split('/');
        let client_name = client_parts.next().ok_or("Missing client name")?.to_string();
        let client_version = client_parts.next().ok_or("Missing client version")?.to_string();

        let mut agent = SpiceUserAgent::default()
            .with_client_name(&client_name)
            .with_client_version(&client_version);

        // Parse optional parts
        let mut extensions = HashMap::new();
        
        for part in parts {
            if part.starts_with('(') && part.ends_with(')') {
                // This is a system field
                let system = part[1..part.len()-1].to_string();
                if agent.client_system.is_none() {
                    agent = agent.with_client_system(&system);
                } else if agent.platform_system.is_none(){
                    agent = agent.with_platform_system(&system);
                }
            } else if part.contains('/') {
                let mut kv = part.split('/');
                if let (Some(key), Some(value)) = (kv.next(), kv.next()) {
                    // Check if this is platform info
                    if agent.platform_name.is_none() {
                        agent = agent.with_platform_name(key)
                            .with_platform_version(value);
                    } else {
                        extensions.insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        if let (Some(platform_name), Some(platform_version)) = (&agent.platform_name, &agent.platform_version) {
            if agent.platform_system.is_none() {
                agent.extensions.insert(platform_name.to_string(), platform_version.to_string());
                agent.platform_name = None;
                agent.platform_version = None;
            }
        }

        if !extensions.is_empty() {
            agent = agent.with_extensions(extensions);
        }

        Ok(agent)
    }
}

#[allow(clippy::must_use_candidate)]
pub fn get_os_type() -> String {
    let os_type = std::env::consts::OS;
    match os_type {
        "" => "unknown".to_string(),
        "macos" => "Darwin".to_string(),    
        "linux" => "Linux".to_string(),
        "windows" => "Windows".to_string(),
        "ios" => "iOS".to_string(),
        "android" => "Android".to_string(),
        "freebsd" => "FreeBSD".to_string(),
        "dragonfly" => "DragonFlyBSD".to_string(),
        "netbsd" => "NetBSD".to_string(),
        "openbsd" => "OpenBSD".to_string(),
        "solaris" => "Solaris".to_string(),
        _ => os_type.to_string(),
    }
}

#[allow(clippy::must_use_candidate)]
pub fn get_os_arch() -> String {
    let os_arch = std::env::consts::ARCH;
    match os_arch {
        "" => "unknown".to_string(),
        "x86" => "i386".to_string(),
        _ => os_arch.to_string(),
    }
}

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[cfg(target_family = "unix")]
fn get_os_release_internal() -> Result<String, GenericError> {
    // call uname -r to get release text
    use std::process::Command;
    let output = Command::new("uname").arg("-r").output()?;
    let release = String::from_utf8(output.stdout)?;

    Ok(release)
}

#[cfg(target_family = "windows")]
fn get_os_release_internal() -> Result<String, GenericError> {
    use winver::WindowsVersion;
    if let Some(version) = WindowsVersion::detect() {
        Ok(version.to_string())
    } else {
        Ok("unknown".to_string())
    }
}

#[allow(clippy::must_use_candidate)]
pub fn get_os_release() -> String {
    get_os_release_internal()
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_string()
}

#[must_use]
pub fn get_platform_os_string() -> String {
    let os_type = get_os_type();
    let os_release = get_os_release();
    let os_arch = get_os_arch();

    format!("{os_type}/{os_release} {os_arch}")
}

#[must_use]
pub fn get_platform_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_spice_user_agent_display() {
        let agent = SpiceUserAgent::default()
            .with_client_name("spice")
            .with_client_version("0.1.0")
            .with_client_system("macos")
            .with_platform_name("spiced")
            .with_platform_version("0.1.0")
            .with_platform_system("macos")
            .with_extensions(
                vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ].into_iter().collect()
            );

        assert_eq!(agent.to_string(), "spice/0.1.0 (macos) spiced/0.1.0 (macos) key1/value1 key2/value2");
    }

    #[test]
    fn test_spice_user_agent_try_from() {
        let agent = SpiceUserAgent::default()
            .with_client_name("spice")
            .with_client_version("0.1.0")
            .with_client_system("macos")
            .with_platform_name("spiced")
            .with_platform_version("0.1.0")
            .with_platform_system("macos")
            .with_extensions(
                vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ].into_iter().collect()
            );

        let agent_str = agent.to_string();
        let parsed_agent = SpiceUserAgent::try_from(agent_str).expect("Failed to parse agent string");

        assert_eq!(agent, parsed_agent);

        let agent_str = "spice/0.1.0 key1/value1";
        let parsed_agent = SpiceUserAgent::try_from(agent_str.to_string()).expect("Failed to parse agent string");
        let agent = SpiceUserAgent::default()
            .with_client_name("spice")
            .with_client_version("0.1.0")
            .with_extensions(
                vec![
                    ("key1".to_string(), "value1".to_string()),
                ].into_iter().collect()
            );
        assert_eq!(agent, parsed_agent);

        let agent_str = "spice/0.1.0 (macos) spiced/0.1.0 (macos) key1/value1 key2/value2";
        let parsed_agent = SpiceUserAgent::try_from(agent_str.to_string()).expect("Failed to parse agent string");
        let agent = SpiceUserAgent::default()
            .with_client_name("spice")
            .with_client_version("0.1.0")
            .with_client_system("macos")
            .with_platform_name("spiced")
            .with_platform_version("0.1.0")
            .with_platform_system("macos")
            .with_extensions(
                vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ].into_iter().collect()
            );
        assert_eq!(agent, parsed_agent);
    }

    #[test]
    fn test_spice_user_agent_merge() {
        let client = SpiceUserAgent::default()
            .with_client_name("spicebench")
            .with_client_version("0.1.0")
            .with_client_system("macos")
            .with_extensions(
                vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ].into_iter().collect()
            );
        let platform = SpiceUserAgent::default()
            .with_platform_name("spiced")
            .with_platform_version("0.1.0")
            .with_platform_system("macos");

        let merged = platform.merge_client(&client);
        let expected = SpiceUserAgent::default()
            .with_client_name("spicebench")
            .with_client_version("0.1.0")
            .with_client_system("macos")
            .with_platform_name("spiced")
            .with_platform_version("0.1.0")
            .with_platform_system("macos")
            .with_extensions(
                vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ].into_iter().collect()
            );
        assert_eq!(expected, merged);
    }
}