//! Structural validation for unpacked and zipped PowerPoint OOXML packages.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    path::Path,
};

use quick_xml::{Reader, events::Event};
use serde::Serialize;

use super::ooxml::{
    OoxmlProfile, OoxmlValidationError, OoxmlValidationSummary, ValidatedOoxmlPackage,
    ValidationState, decoded_attributes, local_name, relationships_part_for_source,
    validate_common,
};

pub use super::ooxml::{
    OoxmlSourceKind as PptxSourceKind, OoxmlValidationIssue as PptxValidationIssue,
};

const PRESENTATION_PART: &str = "ppt/presentation.xml";
const PRESENTATION_RELATIONSHIPS_PART: &str = "ppt/_rels/presentation.xml.rels";
const PRESENTATION_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml";

const PROFILE: OoxmlProfile = OoxmlProfile {
    format: "PowerPoint",
    extension: "pptx",
    main_part: PRESENTATION_PART,
    main_content_type: PRESENTATION_CONTENT_TYPE,
    required_parts: &[
        "[Content_Types].xml",
        "_rels/.rels",
        PRESENTATION_PART,
        PRESENTATION_RELATIONSHIPS_PART,
    ],
    forbidden_parts: &["ppt/vbaProject.bin"],
    expected_root,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PptxValidationReport {
    pub valid: bool,
    pub format: &'static str,
    pub source_kind: PptxSourceKind,
    pub parts: usize,
    pub xml_parts: usize,
    pub relationship_parts: usize,
    pub slides: usize,
    pub total_uncompressed_bytes: u64,
    pub error_count: usize,
    pub warning_count: usize,
    pub issues_truncated: bool,
    pub errors: Vec<PptxValidationIssue>,
    pub warnings: Vec<PptxValidationIssue>,
    pub checks: Vec<&'static str>,
    pub limitations: Vec<&'static str>,
}

impl PptxValidationReport {
    fn from_summary(summary: OoxmlValidationSummary, slides: usize) -> Self {
        Self {
            valid: summary.valid,
            format: "pptx",
            source_kind: summary.source_kind,
            parts: summary.parts,
            xml_parts: summary.xml_parts,
            relationship_parts: summary.relationship_parts,
            slides,
            total_uncompressed_bytes: summary.total_uncompressed_bytes,
            error_count: summary.error_count,
            warning_count: summary.warning_count,
            issues_truncated: summary.issues_truncated,
            errors: summary.errors,
            warnings: summary.warnings,
            checks: summary.checks,
            limitations: summary.limitations,
        }
    }
}

pub type PptxValidationError = OoxmlValidationError;

#[derive(Debug, Default, Clone, Copy)]
pub struct PptxValidator;

impl PptxValidator {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn validate_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<PptxValidationReport, PptxValidationError> {
        let (mut state, package) = validate_common(path.as_ref(), &PROFILE)?;
        let slides = package
            .as_ref()
            .map_or(0, |package| validate_presentation(package, &mut state));
        let summary = state.finish(
            "pptx",
            vec!["presentation_slide_references", "slide_layout_references"],
            vec![
                "OOXML XML Schema conformance is not checked",
                "visual layout, fonts, animations and PowerPoint rendering are not checked",
            ],
        );
        Ok(PptxValidationReport::from_summary(summary, slides))
    }
}

fn expected_root(name: &str) -> Option<&'static str> {
    if name == PRESENTATION_PART {
        Some("presentation")
    } else if name.starts_with("ppt/slides/") && name.ends_with(".xml") {
        Some("sld")
    } else if name.starts_with("ppt/slideLayouts/") && name.ends_with(".xml") {
        Some("sldLayout")
    } else if name.starts_with("ppt/slideMasters/") && name.ends_with(".xml") {
        Some("sldMaster")
    } else if name.starts_with("ppt/notesSlides/") && name.ends_with(".xml") {
        Some("notes")
    } else if name.starts_with("ppt/notesMasters/") && name.ends_with(".xml") {
        Some("notesMaster")
    } else if name.starts_with("ppt/handoutMasters/") && name.ends_with(".xml") {
        Some("handoutMaster")
    } else if name.starts_with("ppt/theme/") && name.ends_with(".xml") {
        Some("theme")
    } else {
        None
    }
}

fn validate_presentation(validated: &ValidatedOoxmlPackage, state: &mut ValidationState) -> usize {
    if !validated.well_formed.contains(PRESENTATION_PART) {
        return 0;
    }
    let Some(bytes) = validated.package.xml(PRESENTATION_PART) else {
        return 0;
    };
    let slide_ids = parse_slide_ids(bytes, state);
    if slide_ids.is_empty() {
        state.warning(
            "presentation_has_no_slides",
            Some(PRESENTATION_PART),
            "presentation does not reference any slides",
        );
    }

    let Some(presentation_relationships) =
        validated.relationships.get(PRESENTATION_RELATIONSHIPS_PART)
    else {
        return slide_ids.len();
    };
    if !slide_ids.is_empty()
        && !presentation_relationships.iter().any(|relationship| {
            relationship.relationship_type.ends_with("/slideMaster") && !relationship.external
        })
    {
        state.error(
            "slide_master_relationship_missing",
            Some(PRESENTATION_RELATIONSHIPS_PART),
            "presentation with slides must reference at least one slide master",
        );
    }

    for relationship_id in slide_ids.values() {
        let Some(relationship) = presentation_relationships
            .iter()
            .find(|relationship| relationship.id == *relationship_id)
        else {
            state.error(
                "slide_relationship_missing",
                Some(PRESENTATION_PART),
                format!("slide references unknown relationship {relationship_id}"),
            );
            continue;
        };
        if !relationship.relationship_type.ends_with("/slide") || relationship.external {
            state.error(
                "slide_relationship_type_invalid",
                Some(PRESENTATION_RELATIONSHIPS_PART),
                format!("relationship {relationship_id} is not an internal slide relationship"),
            );
            continue;
        }
        let Some(slide_part) = relationship.resolved_target.as_deref() else {
            continue;
        };
        let slide_relationships_part = relationships_part_for_source(slide_part);
        let Some(slide_relationships) = validated.relationships.get(&slide_relationships_part)
        else {
            state.error(
                "slide_relationships_part_missing",
                Some(slide_part),
                "slide does not have a relationships part",
            );
            continue;
        };
        if !slide_relationships.iter().any(|relationship| {
            relationship.relationship_type.ends_with("/slideLayout") && !relationship.external
        }) {
            state.error(
                "slide_layout_relationship_missing",
                Some(&slide_relationships_part),
                "slide does not reference a slide layout",
            );
        }
    }
    slide_ids.len()
}

fn parse_slide_ids(bytes: &[u8], state: &mut ValidationState) -> BTreeMap<u32, String> {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut slide_ids = BTreeMap::new();
    let mut relationship_ids = BTreeSet::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element))
                if local_name(element.name().as_ref()) == "sldId" =>
            {
                let Some(attributes) = decoded_attributes(&element) else {
                    buffer.clear();
                    continue;
                };
                let numeric_id = attributes
                    .get("id")
                    .and_then(|value| value.parse::<u32>().ok());
                let relationship_id = attributes
                    .iter()
                    .find(|(key, _)| key.ends_with(":id"))
                    .map(|(_, value)| value.clone());
                let Some(numeric_id) = numeric_id.filter(|id| *id >= 256) else {
                    state.error(
                        "slide_id_invalid",
                        Some(PRESENTATION_PART),
                        "slide id must be an integer greater than or equal to 256",
                    );
                    buffer.clear();
                    continue;
                };
                let Some(relationship_id) = relationship_id.filter(|id| !id.is_empty()) else {
                    state.error(
                        "slide_relationship_id_missing",
                        Some(PRESENTATION_PART),
                        format!("slide id {numeric_id} is missing r:id"),
                    );
                    buffer.clear();
                    continue;
                };
                match slide_ids.entry(numeric_id) {
                    Entry::Occupied(_) => state.error(
                        "duplicate_slide_id",
                        Some(PRESENTATION_PART),
                        format!("duplicate slide id {numeric_id}"),
                    ),
                    Entry::Vacant(entry) => {
                        if !relationship_ids.insert(relationship_id.clone()) {
                            state.error(
                                "duplicate_slide_relationship_id",
                                Some(PRESENTATION_PART),
                                format!("duplicate slide relationship {relationship_id}"),
                            );
                        } else {
                            entry.insert(relationship_id);
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    slide_ids
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::PathBuf};

    use tempfile::tempdir;
    use zip::{ZipWriter, write::SimpleFileOptions};

    use super::*;
    use crate::artifact::ooxml::{CONTENT_TYPES_PART, ROOT_RELATIONSHIPS_PART};

    #[test]
    fn validates_unpacked_and_zipped_presentations() {
        let directory = tempdir().expect("temporary directory");
        let package = directory.path().join("package");
        std::fs::create_dir(&package).expect("package directory");
        write_minimal_package(&package);

        let report = PptxValidator::new()
            .validate_path(&package)
            .expect("validation should run");
        assert!(report.valid, "errors: {:?}", report.errors);
        assert_eq!(report.slides, 1);
        assert_eq!(report.source_kind, PptxSourceKind::Directory);

        let archive = directory.path().join("deck.pptx");
        zip_directory(&package, &archive, None);
        let report = PptxValidator::new()
            .validate_path(&archive)
            .expect("validation should run");
        assert!(report.valid, "errors: {:?}", report.errors);
        assert_eq!(report.source_kind, PptxSourceKind::Archive);
    }

    #[test]
    fn reports_format_and_package_errors() {
        let directory = tempdir().expect("temporary directory");
        let package = directory.path().join("package");
        std::fs::create_dir(&package).expect("package directory");
        write_minimal_package(&package);
        std::fs::write(
            package.join("ppt/slides/_rels/slide1.xml.rels"),
            relationships_xml(&[("rId1", SLIDE_LAYOUT_REL, "../slideLayouts/missing.xml")]),
        )
        .expect("fixture should be overwritten");
        let report = PptxValidator::new()
            .validate_path(&package)
            .expect("validation should run");
        assert!(!report.valid);
        assert!(has_error(&report, "relationship_target_not_found"));

        let archive = directory.path().join("unsafe.pptx");
        zip_directory(&package, &archive, Some("../escape.xml"));
        let report = PptxValidator::new()
            .validate_path(&archive)
            .expect("validation should run");
        assert!(has_error(&report, "invalid_package_part_name"));
    }

    fn has_error(report: &PptxValidationReport, code: &str) -> bool {
        report.errors.iter().any(|issue| issue.code == code)
    }

    const OFFICE_DOCUMENT_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";
    const SLIDE_MASTER_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster";
    const SLIDE_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide";
    const SLIDE_LAYOUT_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout";

    fn write_minimal_package(root: &Path) {
        write_fixture(
            root,
            CONTENT_TYPES_PART,
            &format!(
                r#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/ppt/presentation.xml" ContentType="{PRESENTATION_CONTENT_TYPE}"/></Types>"#
            ),
        );
        write_fixture(
            root,
            ROOT_RELATIONSHIPS_PART,
            &relationships_xml(&[("rId1", OFFICE_DOCUMENT_REL, "ppt/presentation.xml")]),
        );
        write_fixture(
            root,
            PRESENTATION_PART,
            r#"<p:presentation xmlns:p="urn:p" xmlns:r="urn:r"><p:sldIdLst><p:sldId id="256" r:id="rId2"/></p:sldIdLst></p:presentation>"#,
        );
        write_fixture(
            root,
            PRESENTATION_RELATIONSHIPS_PART,
            &relationships_xml(&[
                ("rId1", SLIDE_MASTER_REL, "slideMasters/slideMaster1.xml"),
                ("rId2", SLIDE_REL, "slides/slide1.xml"),
            ]),
        );
        write_fixture(root, "ppt/slides/slide1.xml", r#"<p:sld xmlns:p="urn:p"/>"#);
        write_fixture(
            root,
            "ppt/slides/_rels/slide1.xml.rels",
            &relationships_xml(&[("rId1", SLIDE_LAYOUT_REL, "../slideLayouts/slideLayout1.xml")]),
        );
        write_fixture(
            root,
            "ppt/slideLayouts/slideLayout1.xml",
            r#"<p:sldLayout xmlns:p="urn:p"/>"#,
        );
        write_fixture(
            root,
            "ppt/slideMasters/slideMaster1.xml",
            r#"<p:sldMaster xmlns:p="urn:p"/>"#,
        );
    }

    fn relationships_xml(relationships: &[(&str, &str, &str)]) -> String {
        let entries = relationships
            .iter()
            .map(|(id, relationship_type, target)| {
                format!(r#"<Relationship Id="{id}" Type="{relationship_type}" Target="{target}"/>"#)
            })
            .collect::<Vec<_>>()
            .join("");
        format!(r#"<Relationships xmlns="urn:relationships">{entries}</Relationships>"#)
    }

    fn write_fixture(root: &Path, relative: &str, content: &str) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("fixture parent");
        }
        std::fs::write(path, content).expect("fixture file");
    }

    fn zip_directory(root: &Path, destination: &Path, extra_entry: Option<&str>) {
        let file = File::create(destination).expect("zip destination");
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default();
        let mut files = Vec::new();
        collect_files(root, root, &mut files);
        files.sort();
        for relative in files {
            let name = relative
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            writer.start_file(name, options).expect("zip entry");
            let bytes = std::fs::read(root.join(&relative)).expect("fixture bytes");
            writer.write_all(&bytes).expect("zip bytes");
        }
        if let Some(extra_entry) = extra_entry {
            writer
                .start_file(extra_entry, options)
                .expect("extra entry");
            writer.write_all(b"<escape/>").expect("extra bytes");
        }
        writer.finish().expect("finish zip");
    }

    fn collect_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("fixture directory") {
            let path = entry.expect("fixture entry").path();
            if path.is_dir() {
                collect_files(root, &path, files);
            } else {
                files.push(
                    path.strip_prefix(root)
                        .expect("fixture relative")
                        .to_path_buf(),
                );
            }
        }
    }
}
