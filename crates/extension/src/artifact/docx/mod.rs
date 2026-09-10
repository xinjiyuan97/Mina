//! Structural validation for unpacked and zipped WordprocessingML packages.

use std::{collections::BTreeSet, path::Path};

use quick_xml::{Reader, events::Event};
use serde::Serialize;

use super::ooxml::{
    OoxmlProfile, OoxmlValidationError, OoxmlValidationSummary, ValidatedOoxmlPackage,
    ValidationState, decoded_attributes, local_name, validate_common,
};

pub use super::ooxml::{
    OoxmlSourceKind as DocxSourceKind, OoxmlValidationIssue as DocxValidationIssue,
};

const DOCUMENT_PART: &str = "word/document.xml";
const DOCUMENT_RELATIONSHIPS_PART: &str = "word/_rels/document.xml.rels";
const DOCUMENT_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml";

const PROFILE: OoxmlProfile = OoxmlProfile {
    format: "Word",
    extension: "docx",
    main_part: DOCUMENT_PART,
    main_content_type: DOCUMENT_CONTENT_TYPE,
    required_parts: &["[Content_Types].xml", "_rels/.rels", DOCUMENT_PART],
    forbidden_parts: &["word/vbaProject.bin"],
    expected_root,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DocxValidationReport {
    pub valid: bool,
    pub format: &'static str,
    pub source_kind: DocxSourceKind,
    pub parts: usize,
    pub xml_parts: usize,
    pub relationship_parts: usize,
    pub paragraphs: usize,
    pub tables: usize,
    pub bookmarks: usize,
    pub relationship_references: usize,
    pub total_uncompressed_bytes: u64,
    pub error_count: usize,
    pub warning_count: usize,
    pub issues_truncated: bool,
    pub errors: Vec<DocxValidationIssue>,
    pub warnings: Vec<DocxValidationIssue>,
    pub checks: Vec<&'static str>,
    pub limitations: Vec<&'static str>,
}

impl DocxValidationReport {
    fn from_summary(summary: OoxmlValidationSummary, counts: DocumentCounts) -> Self {
        Self {
            valid: summary.valid,
            format: "docx",
            source_kind: summary.source_kind,
            parts: summary.parts,
            xml_parts: summary.xml_parts,
            relationship_parts: summary.relationship_parts,
            paragraphs: counts.paragraphs,
            tables: counts.tables,
            bookmarks: counts.bookmarks,
            relationship_references: counts.relationship_references,
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

pub type DocxValidationError = OoxmlValidationError;

#[derive(Debug, Default, Clone, Copy)]
pub struct DocxValidator;

impl DocxValidator {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn validate_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<DocxValidationReport, DocxValidationError> {
        let (mut state, package) = validate_common(path.as_ref(), &PROFILE)?;
        let counts = package
            .as_ref()
            .map_or_else(DocumentCounts::default, |package| {
                validate_document(package, &mut state)
            });
        let summary = state.finish(
            "docx",
            vec![
                "document_body_structure",
                "bookmark_pairs",
                "document_relationship_references",
            ],
            vec![
                "OOXML XML Schema conformance is not checked",
                "fields, tracked changes and application-level semantics are not checked",
                "pagination, visual layout and Word rendering are not checked",
            ],
        );
        Ok(DocxValidationReport::from_summary(summary, counts))
    }
}

fn expected_root(name: &str) -> Option<&'static str> {
    if name == DOCUMENT_PART {
        Some("document")
    } else if name.starts_with("word/header") && name.ends_with(".xml") {
        Some("hdr")
    } else if name.starts_with("word/footer") && name.ends_with(".xml") {
        Some("ftr")
    } else {
        match name {
            "word/styles.xml" => Some("styles"),
            "word/numbering.xml" => Some("numbering"),
            "word/settings.xml" => Some("settings"),
            "word/fontTable.xml" => Some("fonts"),
            "word/footnotes.xml" => Some("footnotes"),
            "word/endnotes.xml" => Some("endnotes"),
            "word/comments.xml" => Some("comments"),
            _ if name.starts_with("word/theme/") && name.ends_with(".xml") => Some("theme"),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct DocumentCounts {
    paragraphs: usize,
    tables: usize,
    bookmarks: usize,
    relationship_references: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RelationshipReference {
    id: String,
    attribute: &'static str,
}

fn validate_document(
    validated: &ValidatedOoxmlPackage,
    state: &mut ValidationState,
) -> DocumentCounts {
    let Some(bytes) = validated
        .well_formed
        .contains(DOCUMENT_PART)
        .then(|| validated.package.xml(DOCUMENT_PART))
        .flatten()
    else {
        return DocumentCounts::default();
    };
    let (counts, references) = parse_document(bytes, state);
    if !references.is_empty() {
        let Some(relationships) = validated.relationships.get(DOCUMENT_RELATIONSHIPS_PART) else {
            state.error(
                "document_relationships_part_missing",
                Some(DOCUMENT_PART),
                "document uses relationship attributes but has no relationships part",
            );
            return counts;
        };
        for reference in references {
            let Some(relationship) = relationships
                .iter()
                .find(|relationship| relationship.id == reference.id)
            else {
                state.error(
                    "document_relationship_missing",
                    Some(DOCUMENT_PART),
                    format!(
                        "{} references unknown relationship {}",
                        reference.attribute, reference.id
                    ),
                );
                continue;
            };
            if reference.attribute == "r:embed" && relationship.external {
                state.error(
                    "embedded_relationship_external",
                    Some(DOCUMENT_RELATIONSHIPS_PART),
                    format!(
                        "r:embed {} must reference an internal package part",
                        reference.id
                    ),
                );
            }
        }
    }
    counts
}

fn parse_document(
    bytes: &[u8],
    state: &mut ValidationState,
) -> (DocumentCounts, BTreeSet<RelationshipReference>) {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut bodies = 0;
    let mut paragraphs = 0;
    let mut tables = 0;
    let mut bookmark_starts = BTreeSet::new();
    let mut bookmark_ends = BTreeSet::new();
    let mut references = BTreeSet::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element)) => {
                let qualified_name = element.name();
                let name = local_name(qualified_name.as_ref());
                match name {
                    "body" => bodies += 1,
                    "p" => paragraphs += 1,
                    "tbl" => tables += 1,
                    "bookmarkStart" => collect_bookmark(
                        &element,
                        "bookmark_start_id_missing",
                        "duplicate_bookmark_start",
                        &mut bookmark_starts,
                        state,
                    ),
                    "bookmarkEnd" => collect_bookmark(
                        &element,
                        "bookmark_end_id_missing",
                        "duplicate_bookmark_end",
                        &mut bookmark_ends,
                        state,
                    ),
                    _ => {}
                }
                collect_relationship_references(&element, &mut references);
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    if bodies != 1 {
        state.error(
            "document_body_count_invalid",
            Some(DOCUMENT_PART),
            format!("document must contain exactly one body element, found {bodies}"),
        );
    }
    for id in bookmark_starts.difference(&bookmark_ends) {
        state.error(
            "bookmark_end_missing",
            Some(DOCUMENT_PART),
            format!("bookmark {id} has no matching bookmarkEnd"),
        );
    }
    for id in bookmark_ends.difference(&bookmark_starts) {
        state.error(
            "bookmark_start_missing",
            Some(DOCUMENT_PART),
            format!("bookmark {id} has no matching bookmarkStart"),
        );
    }
    (
        DocumentCounts {
            paragraphs,
            tables,
            bookmarks: bookmark_starts.intersection(&bookmark_ends).count(),
            relationship_references: references.len(),
        },
        references,
    )
}

fn collect_bookmark(
    element: &quick_xml::events::BytesStart<'_>,
    missing_code: &str,
    duplicate_code: &str,
    ids: &mut BTreeSet<String>,
    state: &mut ValidationState,
) {
    let attributes = decoded_attributes(element).unwrap_or_default();
    let id = attributes
        .iter()
        .find(|(name, _)| local_name(name) == "id")
        .map(|(_, value)| value);
    match id.filter(|id| !id.is_empty()) {
        Some(id) if ids.insert(id.clone()) => {}
        Some(id) => state.error(
            duplicate_code,
            Some(DOCUMENT_PART),
            format!("bookmark id {id} is repeated"),
        ),
        None => state.error(
            missing_code,
            Some(DOCUMENT_PART),
            "bookmark marker is missing its id",
        ),
    }
}

fn collect_relationship_references(
    element: &quick_xml::events::BytesStart<'_>,
    references: &mut BTreeSet<RelationshipReference>,
) {
    let Some(attributes) = decoded_attributes(element) else {
        return;
    };
    for attribute in ["r:id", "r:embed", "r:link"] {
        if let Some(id) = attributes.get(attribute).filter(|id| !id.is_empty()) {
            references.insert(RelationshipReference {
                id: id.clone(),
                attribute,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::PathBuf};

    use tempfile::tempdir;
    use zip::{ZipWriter, write::SimpleFileOptions};

    use super::*;

    #[test]
    fn validates_unpacked_and_zipped_documents() {
        let directory = tempdir().expect("temporary directory");
        let package = directory.path().join("document");
        std::fs::create_dir(&package).expect("package directory");
        write_minimal_package(&package);

        let report = DocxValidator::new()
            .validate_path(&package)
            .expect("validation should run");
        assert!(report.valid, "errors: {:?}", report.errors);
        assert_eq!(report.paragraphs, 2);
        assert_eq!(report.tables, 1);
        assert_eq!(report.bookmarks, 1);

        let archive = directory.path().join("document.docx");
        zip_directory(&package, &archive);
        let report = DocxValidator::new()
            .validate_path(&archive)
            .expect("validation should run");
        assert!(report.valid, "errors: {:?}", report.errors);
        assert_eq!(report.source_kind, DocxSourceKind::Archive);
    }

    #[test]
    fn reports_body_bookmark_and_relationship_reference_errors() {
        let directory = tempdir().expect("temporary directory");
        write_minimal_package(directory.path());
        std::fs::write(
            directory.path().join(DOCUMENT_PART),
            r#"<w:document xmlns:w="urn:w" xmlns:r="urn:r"><w:body><w:p><w:bookmarkStart w:id="7"/><w:hyperlink r:id="missing"/></w:p></w:body><w:body/></w:document>"#,
        )
        .expect("overwrite fixture");
        let report = DocxValidator::new()
            .validate_path(directory.path())
            .expect("validation should run");
        assert!(!report.valid);
        for code in [
            "document_body_count_invalid",
            "bookmark_end_missing",
            "document_relationships_part_missing",
        ] {
            assert!(
                report.errors.iter().any(|issue| issue.code == code),
                "missing {code}: {:?}",
                report.errors
            );
        }
    }

    const OFFICE_DOCUMENT_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";

    fn write_minimal_package(root: &Path) {
        write_fixture(
            root,
            "[Content_Types].xml",
            &format!(
                r#"<Types><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="{DOCUMENT_CONTENT_TYPE}"/></Types>"#
            ),
        );
        write_fixture(
            root,
            "_rels/.rels",
            &format!(
                r#"<Relationships><Relationship Id="rId1" Type="{OFFICE_DOCUMENT_REL}" Target="word/document.xml"/></Relationships>"#
            ),
        );
        write_fixture(
            root,
            DOCUMENT_PART,
            r#"<w:document xmlns:w="urn:w"><w:body><w:p><w:bookmarkStart w:id="7"/><w:r/><w:bookmarkEnd w:id="7"/></w:p><w:tbl><w:tr><w:tc><w:p/></w:tc></w:tr></w:tbl></w:body></w:document>"#,
        );
    }

    fn write_fixture(root: &Path, relative: &str, content: &str) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("fixture parent");
        }
        std::fs::write(path, content).expect("fixture file");
    }

    fn zip_directory(root: &Path, destination: &Path) {
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
            writer
                .write_all(&std::fs::read(root.join(relative)).expect("fixture bytes"))
                .expect("zip bytes");
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
