//! Shared Open Packaging Conventions checks for OOXML document formats.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, Read},
    path::Path,
};

use quick_xml::{Reader, XmlVersion, events::Event};
use serde::Serialize;
use thiserror::Error;
use zip::ZipArchive;

const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PART_BYTES: u64 = 64 * 1024 * 1024;
const MAX_XML_PART_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;
const MAX_REPORTED_ISSUES_PER_LEVEL: usize = 100;

pub(crate) const CONTENT_TYPES_PART: &str = "[Content_Types].xml";
pub(crate) const ROOT_RELATIONSHIPS_PART: &str = "_rels/.rels";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OoxmlSourceKind {
    Directory,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OoxmlValidationIssue {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OoxmlValidationSummary {
    pub valid: bool,
    pub format: &'static str,
    pub source_kind: OoxmlSourceKind,
    pub parts: usize,
    pub xml_parts: usize,
    pub relationship_parts: usize,
    pub total_uncompressed_bytes: u64,
    pub error_count: usize,
    pub warning_count: usize,
    pub issues_truncated: bool,
    pub errors: Vec<OoxmlValidationIssue>,
    pub warnings: Vec<OoxmlValidationIssue>,
    pub checks: Vec<&'static str>,
    pub limitations: Vec<&'static str>,
}

#[derive(Debug, Error)]
pub enum OoxmlValidationError {
    #[error("the OOXML source could not be read")]
    Io(#[source] io::Error),
    #[error("the OOXML source must be a regular file or directory")]
    UnsupportedSource,
}

pub(crate) struct OoxmlProfile {
    pub format: &'static str,
    pub extension: &'static str,
    pub main_part: &'static str,
    pub main_content_type: &'static str,
    pub required_parts: &'static [&'static str],
    pub forbidden_parts: &'static [&'static str],
    pub expected_root: fn(&str) -> Option<&'static str>,
}

#[derive(Debug)]
pub(crate) struct ValidationState {
    source_kind: OoxmlSourceKind,
    parts: usize,
    xml_parts: usize,
    relationship_parts: usize,
    total_uncompressed_bytes: u64,
    error_count: usize,
    warning_count: usize,
    issues_truncated: bool,
    errors: Vec<OoxmlValidationIssue>,
    warnings: Vec<OoxmlValidationIssue>,
}

impl ValidationState {
    fn new(source_kind: OoxmlSourceKind) -> Self {
        Self {
            source_kind,
            parts: 0,
            xml_parts: 0,
            relationship_parts: 0,
            total_uncompressed_bytes: 0,
            error_count: 0,
            warning_count: 0,
            issues_truncated: false,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub(crate) fn error(
        &mut self,
        code: impl Into<String>,
        part: Option<&str>,
        message: impl Into<String>,
    ) {
        self.error_count += 1;
        if self.errors.len() < MAX_REPORTED_ISSUES_PER_LEVEL {
            self.errors.push(OoxmlValidationIssue {
                code: code.into(),
                part: part.map(str::to_owned),
                message: message.into(),
            });
        } else {
            self.issues_truncated = true;
        }
    }

    pub(crate) fn warning(
        &mut self,
        code: impl Into<String>,
        part: Option<&str>,
        message: impl Into<String>,
    ) {
        self.warning_count += 1;
        if self.warnings.len() < MAX_REPORTED_ISSUES_PER_LEVEL {
            self.warnings.push(OoxmlValidationIssue {
                code: code.into(),
                part: part.map(str::to_owned),
                message: message.into(),
            });
        } else {
            self.issues_truncated = true;
        }
    }

    pub(crate) fn finish(
        self,
        format: &'static str,
        mut checks: Vec<&'static str>,
        limitations: Vec<&'static str>,
    ) -> OoxmlValidationSummary {
        let mut common_checks = vec![
            "package_structure",
            "xml_well_formedness",
            "content_type_coverage",
            "relationship_targets",
            "root_office_document",
        ];
        common_checks.append(&mut checks);
        OoxmlValidationSummary {
            valid: self.error_count == 0,
            format,
            source_kind: self.source_kind,
            parts: self.parts,
            xml_parts: self.xml_parts,
            relationship_parts: self.relationship_parts,
            total_uncompressed_bytes: self.total_uncompressed_bytes,
            error_count: self.error_count,
            warning_count: self.warning_count,
            issues_truncated: self.issues_truncated,
            errors: self.errors,
            warnings: self.warnings,
            checks: common_checks,
            limitations,
        }
    }
}

#[derive(Debug)]
struct PackagePart {
    xml: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
pub(crate) struct OoxmlPackage {
    parts: BTreeMap<String, PackagePart>,
}

impl OoxmlPackage {
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.parts.contains_key(name)
    }

    pub(crate) fn xml(&self, name: &str) -> Option<&[u8]> {
        self.parts.get(name)?.xml.as_deref()
    }

    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.parts.keys().map(String::as_str)
    }

    fn insert(
        &mut self,
        name: String,
        xml: Option<Vec<u8>>,
        state: &mut ValidationState,
        case_names: &mut BTreeMap<String, String>,
    ) {
        let lowercase = name.to_ascii_lowercase();
        if let Some(existing) = case_names.get(&lowercase) {
            state.error(
                "duplicate_package_part",
                Some(&name),
                format!("package part conflicts with {existing}"),
            );
            return;
        }
        case_names.insert(lowercase, name.clone());
        self.parts.insert(name, PackagePart { xml });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Relationship {
    pub id: String,
    pub relationship_type: String,
    pub resolved_target: Option<String>,
    pub external: bool,
}

pub(crate) type RelationshipMap = BTreeMap<String, Vec<Relationship>>;

#[derive(Debug)]
pub(crate) struct ValidatedOoxmlPackage {
    pub package: OoxmlPackage,
    pub well_formed: BTreeSet<String>,
    pub relationships: RelationshipMap,
}

pub(crate) fn validate_common(
    path: &Path,
    profile: &OoxmlProfile,
) -> Result<(ValidationState, Option<ValidatedOoxmlPackage>), OoxmlValidationError> {
    let metadata = std::fs::metadata(path).map_err(OoxmlValidationError::Io)?;
    let source_kind = if metadata.is_dir() {
        OoxmlSourceKind::Directory
    } else if metadata.is_file() {
        OoxmlSourceKind::Archive
    } else {
        return Err(OoxmlValidationError::UnsupportedSource);
    };
    let mut state = ValidationState::new(source_kind);
    let package = match source_kind {
        OoxmlSourceKind::Directory => load_directory(path, &mut state)?,
        OoxmlSourceKind::Archive => {
            load_archive(path, metadata.len(), profile.extension, &mut state)?
        }
    };
    let Some(package) = package else {
        return Ok((state, None));
    };

    state.parts = package.parts.len();
    state.xml_parts = package
        .parts
        .keys()
        .filter(|name| is_xml_part(name))
        .count();
    state.relationship_parts = package
        .parts
        .keys()
        .filter(|name| name.ends_with(".rels"))
        .count();

    for required in profile.required_parts {
        if !package.contains(required) {
            state.error(
                "missing_required_part",
                Some(required),
                format!("required {} package part is missing", profile.format),
            );
        }
    }
    for forbidden in profile.forbidden_parts {
        if let Some(actual) = package
            .names()
            .find(|name| name.eq_ignore_ascii_case(forbidden))
        {
            state.error(
                "macro_content_not_allowed",
                Some(actual),
                format!(
                    "a .{} package must not contain VBA content",
                    profile.extension
                ),
            );
        }
    }

    let mut well_formed = BTreeSet::new();
    for name in package.parts.keys().filter(|name| is_xml_part(name)) {
        let Some(bytes) = package.xml(name) else {
            continue;
        };
        if let Some(root) = validate_xml(name, bytes, &mut state) {
            well_formed.insert(name.clone());
            let expected = if name == CONTENT_TYPES_PART {
                Some("Types")
            } else if name.ends_with(".rels") {
                Some("Relationships")
            } else {
                (profile.expected_root)(name)
            };
            if let Some(expected) = expected
                && root != expected
            {
                state.error(
                    "unexpected_xml_root",
                    Some(name),
                    format!("expected root element {expected}, found {root}"),
                );
            }
        }
    }

    validate_content_types(&package, &well_formed, profile, &mut state);
    let relationships = validate_relationships(&package, &well_formed, &mut state);
    validate_root_office_document(&relationships, profile.main_part, &mut state);
    Ok((
        state,
        Some(ValidatedOoxmlPackage {
            package,
            well_formed,
            relationships,
        }),
    ))
}

fn load_directory(
    root: &Path,
    state: &mut ValidationState,
) -> Result<Option<OoxmlPackage>, OoxmlValidationError> {
    if !root.join(CONTENT_TYPES_PART).is_file() {
        state.error(
            "missing_content_types_part",
            Some(CONTENT_TYPES_PART),
            "the directory is not an unpacked OOXML package",
        );
        return Ok(None);
    }
    let mut package = OoxmlPackage::default();
    let mut case_names = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    let mut total = 0_u64;
    let mut entries_seen = 0_usize;

    while let Some(directory) = stack.pop() {
        let entries = std::fs::read_dir(&directory).map_err(OoxmlValidationError::Io)?;
        for entry in entries {
            let entry = entry.map_err(OoxmlValidationError::Io)?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(OoxmlValidationError::Io)?;
            if metadata.file_type().is_symlink() {
                let part = display_part_path(root, &path);
                state.error(
                    "package_symlink_not_allowed",
                    part.as_deref(),
                    "unpacked OOXML packages must not contain symbolic links",
                );
                continue;
            }
            if metadata.is_dir() {
                stack.push(path);
                continue;
            }
            entries_seen += 1;
            if entries_seen > MAX_PARTS {
                state.error(
                    "package_part_limit_exceeded",
                    None,
                    format!("package contains more than {MAX_PARTS} parts"),
                );
                state.total_uncompressed_bytes = total;
                return Ok(Some(package));
            }
            if !metadata.is_file() {
                let part = display_part_path(root, &path);
                state.error(
                    "unsupported_package_entry",
                    part.as_deref(),
                    "package entry is not a regular file",
                );
                continue;
            }
            let Some(name) = display_part_path(root, &path) else {
                state.error(
                    "invalid_package_part_name",
                    None,
                    "package contains a non-UTF-8 part name",
                );
                continue;
            };
            if let Err(message) = validate_part_name(&name) {
                state.error("invalid_package_part_name", Some(&name), message);
                continue;
            }
            let size = metadata.len();
            total = total.saturating_add(size);
            if size > MAX_PART_BYTES {
                state.error(
                    "package_part_too_large",
                    Some(&name),
                    format!("package part exceeds {MAX_PART_BYTES} bytes"),
                );
                continue;
            }
            if total > MAX_TOTAL_UNCOMPRESSED_BYTES {
                state.total_uncompressed_bytes = total;
                state.error(
                    "package_uncompressed_size_exceeded",
                    None,
                    format!("package expands beyond {MAX_TOTAL_UNCOMPRESSED_BYTES} bytes"),
                );
                return Ok(Some(package));
            }
            let xml = read_xml_part(&path, &name, size, state)?;
            package.insert(name, xml, state, &mut case_names);
        }
    }
    state.total_uncompressed_bytes = total;
    Ok(Some(package))
}

fn read_xml_part(
    path: &Path,
    name: &str,
    size: u64,
    state: &mut ValidationState,
) -> Result<Option<Vec<u8>>, OoxmlValidationError> {
    if !is_xml_part(name) {
        return Ok(None);
    }
    if size > MAX_XML_PART_BYTES {
        state.error(
            "xml_part_too_large",
            Some(name),
            format!("XML part exceeds {MAX_XML_PART_BYTES} bytes"),
        );
        return Ok(None);
    }
    std::fs::read(path)
        .map(Some)
        .map_err(OoxmlValidationError::Io)
}

fn load_archive(
    path: &Path,
    archive_bytes: u64,
    expected_extension: &str,
    state: &mut ValidationState,
) -> Result<Option<OoxmlPackage>, OoxmlValidationError> {
    if archive_bytes > MAX_ARCHIVE_BYTES {
        state.error(
            "ooxml_archive_too_large",
            None,
            format!("OOXML archive exceeds {MAX_ARCHIVE_BYTES} bytes"),
        );
        return Ok(None);
    }
    if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_none_or(|extension| !extension.eq_ignore_ascii_case(expected_extension))
    {
        state.warning(
            "unexpected_file_extension",
            None,
            format!("archive does not use the .{expected_extension} extension"),
        );
    }

    let file = File::open(path).map_err(OoxmlValidationError::Io)?;
    let mut archive = match ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(_) => {
            state.error(
                "invalid_ooxml_archive",
                None,
                "file is not a readable ZIP archive",
            );
            return Ok(None);
        }
    };
    if archive.len() > MAX_PARTS {
        state.error(
            "package_part_limit_exceeded",
            None,
            format!("package contains more than {MAX_PARTS} entries"),
        );
        return Ok(None);
    }

    let mut package = OoxmlPackage::default();
    let mut case_names = BTreeMap::new();
    let mut total = 0_u64;
    for index in 0..archive.len() {
        let mut file = match archive.by_index(index) {
            Ok(file) => file,
            Err(_) => {
                state.error(
                    "zip_entry_unreadable",
                    None,
                    format!("ZIP entry {index} could not be opened"),
                );
                continue;
            }
        };
        let name = file.name().to_owned();
        if file.is_dir() {
            if let Err(message) = validate_directory_entry_name(&name) {
                state.error("invalid_package_part_name", Some(&name), message);
            }
            continue;
        }
        if file.encrypted() {
            state.error(
                "encrypted_package_part",
                Some(&name),
                "encrypted ZIP entries are not allowed in OOXML packages",
            );
            continue;
        }
        if file.is_symlink() {
            state.error(
                "package_symlink_not_allowed",
                Some(&name),
                "OOXML packages must not contain symbolic-link entries",
            );
            continue;
        }
        if let Err(message) = validate_part_name(&name) {
            state.error("invalid_package_part_name", Some(&name), message);
            continue;
        }
        let size = file.size();
        total = total.saturating_add(size);
        if size > MAX_PART_BYTES {
            state.error(
                "package_part_too_large",
                Some(&name),
                format!("package part exceeds {MAX_PART_BYTES} bytes"),
            );
            continue;
        }
        if total > MAX_TOTAL_UNCOMPRESSED_BYTES {
            state.total_uncompressed_bytes = total;
            state.error(
                "package_uncompressed_size_exceeded",
                None,
                format!("package expands beyond {MAX_TOTAL_UNCOMPRESSED_BYTES} bytes"),
            );
            return Ok(Some(package));
        }

        let xml = if is_xml_part(&name) {
            if size > MAX_XML_PART_BYTES {
                state.error(
                    "xml_part_too_large",
                    Some(&name),
                    format!("XML part exceeds {MAX_XML_PART_BYTES} bytes"),
                );
                drain_zip_entry(&mut file, &name, state);
                None
            } else {
                let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
                match file.read_to_end(&mut bytes) {
                    Ok(_) => Some(bytes),
                    Err(_) => {
                        state.error(
                            "zip_entry_unreadable",
                            Some(&name),
                            "ZIP entry data or checksum is invalid",
                        );
                        None
                    }
                }
            }
        } else {
            drain_zip_entry(&mut file, &name, state);
            None
        };
        package.insert(name, xml, state, &mut case_names);
    }
    state.total_uncompressed_bytes = total;
    Ok(Some(package))
}

fn drain_zip_entry<R: Read>(file: &mut R, name: &str, state: &mut ValidationState) {
    if io::copy(file, &mut io::sink()).is_err() {
        state.error(
            "zip_entry_unreadable",
            Some(name),
            "ZIP entry data or checksum is invalid",
        );
    }
}

fn display_part_path(root: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(root)
        .ok()?
        .to_str()
        .map(|part| part.replace(std::path::MAIN_SEPARATOR, "/"))
}

fn validate_directory_entry_name(name: &str) -> Result<(), &'static str> {
    let trimmed = name.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("package directory name must not be empty");
    }
    validate_part_name(trimmed)
}

fn validate_part_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() || name.starts_with('/') || name.ends_with('/') {
        return Err("package part name must be a non-empty relative path");
    }
    if name.contains('\\') || name.contains('\0') {
        return Err("package part name contains a forbidden character");
    }
    if name
        .split('/')
        .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        return Err("package part name contains an invalid path segment");
    }
    Ok(())
}

fn is_xml_part(name: &str) -> bool {
    name == CONTENT_TYPES_PART || name.ends_with(".xml") || name.ends_with(".rels")
}

fn validate_xml(name: &str, bytes: &[u8], state: &mut ValidationState) -> Option<String> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().check_comments = true;
    let mut buffer = Vec::new();
    let mut depth = 0_usize;
    let mut roots = 0_usize;
    let mut root_name = None;

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) => {
                if !validate_attributes(&element, name, state) {
                    return None;
                }
                if depth == 0 {
                    roots += 1;
                    root_name = Some(local_name(element.name().as_ref()).to_owned());
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) => {
                if !validate_attributes(&element, name, state) {
                    return None;
                }
                if depth == 0 {
                    roots += 1;
                    root_name = Some(local_name(element.name().as_ref()).to_owned());
                }
            }
            Ok(Event::End(_)) => depth = depth.saturating_sub(1),
            Ok(Event::DocType(_)) => {
                state.error(
                    "xml_doctype_not_allowed",
                    Some(name),
                    "OOXML parts must not contain a document type declaration",
                );
                return None;
            }
            Ok(Event::Text(text))
                if depth == 0 && !text.as_ref().chars().all(char::is_whitespace) =>
            {
                state.error(
                    "xml_text_outside_root",
                    Some(name),
                    "XML part contains text outside its root element",
                );
                return None;
            }
            Ok(Event::GeneralRef(_)) if depth == 0 => {
                state.error(
                    "xml_reference_outside_root",
                    Some(name),
                    "XML part contains an entity reference outside its root element",
                );
                return None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                state.error(
                    "malformed_xml",
                    Some(name),
                    format!(
                        "XML parsing failed near byte {}: {error}",
                        reader.error_position()
                    ),
                );
                return None;
            }
        }
        buffer.clear();
    }
    if roots != 1 || depth != 0 {
        state.error(
            "malformed_xml_document",
            Some(name),
            "XML part must contain exactly one balanced root element",
        );
        return None;
    }
    root_name
}

fn validate_attributes(
    element: &quick_xml::events::BytesStart<'_>,
    part: &str,
    state: &mut ValidationState,
) -> bool {
    for attribute in element.attributes().with_checks(true) {
        let Ok(attribute) = attribute else {
            state.error(
                "malformed_xml_attribute",
                Some(part),
                "XML element contains an invalid or duplicate attribute",
            );
            return false;
        };
        if attribute.normalized_value(XmlVersion::Implicit1_0).is_err() {
            state.error(
                "malformed_xml_attribute",
                Some(part),
                "XML attribute value is not valid",
            );
            return false;
        }
    }
    true
}

#[derive(Debug, Default)]
struct ContentTypes {
    defaults: BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
}

fn validate_content_types(
    package: &OoxmlPackage,
    well_formed: &BTreeSet<String>,
    profile: &OoxmlProfile,
    state: &mut ValidationState,
) {
    if !well_formed.contains(CONTENT_TYPES_PART) {
        return;
    }
    let Some(bytes) = package.xml(CONTENT_TYPES_PART) else {
        return;
    };
    let Some(content_types) = parse_content_types(bytes, state) else {
        return;
    };
    for name in package.names().filter(|name| *name != CONTENT_TYPES_PART) {
        let override_name = format!("/{name}");
        let extension = name
            .rsplit_once('.')
            .map(|(_, extension)| extension.to_ascii_lowercase());
        let covered = content_types.overrides.contains_key(&override_name)
            || extension
                .as_ref()
                .is_some_and(|extension| content_types.defaults.contains_key(extension));
        if !covered {
            state.error(
                "content_type_missing",
                Some(name),
                "package part is not covered by a Default or Override content type",
            );
        }
    }

    match content_types
        .overrides
        .get(&format!("/{}", profile.main_part))
    {
        Some(content_type) if content_type == profile.main_content_type => {}
        Some(_) => state.error(
            "invalid_main_content_type",
            Some(CONTENT_TYPES_PART),
            format!("{} has the wrong content type", profile.main_part),
        ),
        None => state.error(
            "main_content_type_override_missing",
            Some(CONTENT_TYPES_PART),
            format!(
                "{} requires a format-specific content type override",
                profile.main_part
            ),
        ),
    }
}

fn parse_content_types(bytes: &[u8], state: &mut ValidationState) -> Option<ContentTypes> {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut content_types = ContentTypes::default();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element)) => {
                match local_name(element.name().as_ref()) {
                    "Default" => {
                        let attributes = decoded_attributes(&element)?;
                        let Some(extension) = attributes.get("Extension") else {
                            state.error(
                                "content_type_default_invalid",
                                Some(CONTENT_TYPES_PART),
                                "Default content type is missing Extension",
                            );
                            buffer.clear();
                            continue;
                        };
                        let Some(content_type) = attributes.get("ContentType") else {
                            state.error(
                                "content_type_default_invalid",
                                Some(CONTENT_TYPES_PART),
                                "Default content type is missing ContentType",
                            );
                            buffer.clear();
                            continue;
                        };
                        let extension = extension.to_ascii_lowercase();
                        if content_types
                            .defaults
                            .insert(extension.clone(), content_type.clone())
                            .is_some()
                        {
                            state.error(
                                "duplicate_content_type_default",
                                Some(CONTENT_TYPES_PART),
                                format!("duplicate Default content type for extension {extension}"),
                            );
                        }
                    }
                    "Override" => {
                        let attributes = decoded_attributes(&element)?;
                        let Some(part_name) = attributes.get("PartName") else {
                            state.error(
                                "content_type_override_invalid",
                                Some(CONTENT_TYPES_PART),
                                "Override content type is missing PartName",
                            );
                            buffer.clear();
                            continue;
                        };
                        let Some(content_type) = attributes.get("ContentType") else {
                            state.error(
                                "content_type_override_invalid",
                                Some(CONTENT_TYPES_PART),
                                "Override content type is missing ContentType",
                            );
                            buffer.clear();
                            continue;
                        };
                        if !part_name.starts_with('/') {
                            state.error(
                                "content_type_part_name_invalid",
                                Some(CONTENT_TYPES_PART),
                                format!("Override PartName must start with '/': {part_name}"),
                            );
                        }
                        if content_types
                            .overrides
                            .insert(part_name.clone(), content_type.clone())
                            .is_some()
                        {
                            state.error(
                                "duplicate_content_type_override",
                                Some(CONTENT_TYPES_PART),
                                format!("duplicate Override content type for {part_name}"),
                            );
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => return None,
        }
        buffer.clear();
    }
    Some(content_types)
}

fn validate_relationships(
    package: &OoxmlPackage,
    well_formed: &BTreeSet<String>,
    state: &mut ValidationState,
) -> RelationshipMap {
    let mut result = BTreeMap::new();
    for name in package.names().filter(|name| name.ends_with(".rels")) {
        if !well_formed.contains(name) {
            continue;
        }
        let Some(source) = relationship_source(name) else {
            state.error(
                "relationship_part_path_invalid",
                Some(name),
                "relationship part is not stored under an _rels directory",
            );
            continue;
        };
        if let Some(source_part) = source.as_deref()
            && !package.contains(source_part)
        {
            state.error(
                "relationship_source_missing",
                Some(name),
                format!("relationship source part does not exist: {source_part}"),
            );
        }
        let Some(bytes) = package.xml(name) else {
            continue;
        };
        let relationships = parse_relationship_part(name, source.as_deref(), bytes, package, state);
        result.insert(name.to_owned(), relationships);
    }
    result
}

fn parse_relationship_part(
    relationship_part: &str,
    source: Option<&str>,
    bytes: &[u8],
    package: &OoxmlPackage,
    state: &mut ValidationState,
) -> Vec<Relationship> {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut ids = BTreeSet::new();
    let mut relationships = Vec::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element))
                if local_name(element.name().as_ref()) == "Relationship" =>
            {
                let Some(attributes) = decoded_attributes(&element) else {
                    buffer.clear();
                    continue;
                };
                let Some(id) = attributes.get("Id").filter(|value| !value.is_empty()) else {
                    state.error(
                        "relationship_id_missing",
                        Some(relationship_part),
                        "Relationship is missing Id",
                    );
                    buffer.clear();
                    continue;
                };
                if !ids.insert(id.clone()) {
                    state.error(
                        "duplicate_relationship_id",
                        Some(relationship_part),
                        format!("duplicate relationship Id {id}"),
                    );
                    buffer.clear();
                    continue;
                }
                let Some(relationship_type) =
                    attributes.get("Type").filter(|value| !value.is_empty())
                else {
                    state.error(
                        "relationship_type_missing",
                        Some(relationship_part),
                        format!("Relationship {id} is missing Type"),
                    );
                    buffer.clear();
                    continue;
                };
                let Some(target) = attributes.get("Target").filter(|value| !value.is_empty())
                else {
                    state.error(
                        "relationship_target_missing",
                        Some(relationship_part),
                        format!("Relationship {id} is missing Target"),
                    );
                    buffer.clear();
                    continue;
                };
                let external = attributes
                    .get("TargetMode")
                    .is_some_and(|mode| mode.eq_ignore_ascii_case("external"));
                if let Some(mode) = attributes.get("TargetMode")
                    && !mode.eq_ignore_ascii_case("external")
                {
                    state.error(
                        "relationship_target_mode_invalid",
                        Some(relationship_part),
                        format!("Relationship {id} has unsupported TargetMode {mode}"),
                    );
                }
                let resolved_target = if external {
                    state.warning(
                        "external_relationship",
                        Some(relationship_part),
                        format!("Relationship {id} references an external resource"),
                    );
                    None
                } else {
                    match resolve_relationship_target(source, target) {
                        Ok(target) => {
                            if !package.contains(&target) {
                                if let Some(case_match) = package
                                    .names()
                                    .find(|name| name.eq_ignore_ascii_case(&target))
                                {
                                    state.error(
                                        "relationship_target_case_mismatch",
                                        Some(relationship_part),
                                        format!(
                                            "Relationship {id} targets {target}, but package contains {case_match}"
                                        ),
                                    );
                                } else {
                                    state.error(
                                        "relationship_target_not_found",
                                        Some(relationship_part),
                                        format!(
                                            "Relationship {id} target does not exist: {target}"
                                        ),
                                    );
                                }
                            }
                            Some(target)
                        }
                        Err(message) => {
                            state.error(
                                "relationship_target_invalid",
                                Some(relationship_part),
                                format!("Relationship {id} has an invalid target: {message}"),
                            );
                            None
                        }
                    }
                };
                relationships.push(Relationship {
                    id: id.clone(),
                    relationship_type: relationship_type.clone(),
                    resolved_target,
                    external,
                });
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    relationships
}

fn validate_root_office_document(
    relationships: &RelationshipMap,
    expected_main_part: &str,
    state: &mut ValidationState,
) {
    let Some(root) = relationships.get(ROOT_RELATIONSHIPS_PART) else {
        return;
    };
    let office_document = root.iter().find(|relationship| {
        relationship.relationship_type.ends_with("/officeDocument") && !relationship.external
    });
    match office_document.and_then(|relationship| relationship.resolved_target.as_deref()) {
        Some(target) if target == expected_main_part => {}
        Some(target) => state.error(
            "office_document_target_invalid",
            Some(ROOT_RELATIONSHIPS_PART),
            format!("officeDocument relationship targets {target} instead of {expected_main_part}"),
        ),
        None => state.error(
            "office_document_relationship_missing",
            Some(ROOT_RELATIONSHIPS_PART),
            "root relationships do not identify the format's main document part",
        ),
    }
}

pub(crate) fn decoded_attributes(
    element: &quick_xml::events::BytesStart<'_>,
) -> Option<BTreeMap<String, String>> {
    let mut attributes = BTreeMap::new();
    for attribute in element.attributes().with_checks(true) {
        let attribute = attribute.ok()?;
        let value = attribute
            .normalized_value(XmlVersion::Implicit1_0)
            .ok()?
            .into_owned();
        attributes.insert(attribute.key.as_ref().to_owned(), value);
    }
    Some(attributes)
}

pub(crate) fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

fn relationship_source(relationship_part: &str) -> Option<Option<String>> {
    if relationship_part == ROOT_RELATIONSHIPS_PART {
        return Some(None);
    }
    let (directory, file) = relationship_part.rsplit_once("/_rels/")?;
    let source_file = file.strip_suffix(".rels")?;
    if source_file.is_empty() {
        return None;
    }
    Some(Some(format!("{directory}/{source_file}")))
}

fn resolve_relationship_target(source: Option<&str>, target: &str) -> Result<String, &'static str> {
    let target = target.split('#').next().unwrap_or_default();
    if target.is_empty() {
        return Err("target resolves to an empty package path");
    }
    if target.contains('?') || target.contains('\\') || target.contains('\0') {
        return Err("target contains a forbidden character");
    }
    let target = percent_decode(target)?;
    let first_segment = target
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or_default();
    if first_segment.contains(':') {
        return Err("internal relationship target must not be an absolute URI");
    }

    let mut segments = if target.starts_with('/') {
        Vec::new()
    } else {
        source
            .map(Path::new)
            .and_then(Path::parent)
            .map(path_segments)
            .unwrap_or_default()
    };
    for segment in target.trim_start_matches('/').split('/') {
        match segment {
            "" => return Err("target contains an empty path segment"),
            "." => {}
            ".." => {
                if segments.pop().is_none() {
                    return Err("target escapes the package root");
                }
            }
            value => segments.push(value.to_owned()),
        }
    }
    if segments.is_empty() {
        return Err("target resolves to the package root");
    }
    Ok(segments.join("/"))
}

fn path_segments(path: &Path) -> Vec<String> {
    path.components()
        .filter_map(|component| component.as_os_str().to_str().map(str::to_owned))
        .collect()
}

fn percent_decode(value: &str) -> Result<String, &'static str> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0_usize;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err("target contains invalid percent encoding");
            }
            let high = hex_value(bytes[index + 1])?;
            let low = hex_value(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "target is not valid UTF-8")
}

fn hex_value(value: u8) -> Result<u8, &'static str> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err("target contains invalid percent encoding"),
    }
}

#[cfg(feature = "pptx")]
pub(crate) fn relationships_part_for_source(source: &str) -> String {
    let path = Path::new(source);
    let directory = path.parent().and_then(Path::to_str).unwrap_or_default();
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if directory.is_empty() {
        format!("_rels/{file}.rels")
    } else {
        format!("{directory}/_rels/{file}.rels")
    }
}
