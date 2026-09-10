//! Structural validation for unpacked and zipped Excel OOXML packages.

use std::{collections::BTreeSet, path::Path};

use quick_xml::{Reader, events::Event};
use serde::Serialize;

use super::ooxml::{
    OoxmlProfile, OoxmlValidationError, OoxmlValidationSummary, Relationship,
    ValidatedOoxmlPackage, ValidationState, decoded_attributes, local_name, validate_common,
};

pub use super::ooxml::{
    OoxmlSourceKind as XlsxSourceKind, OoxmlValidationIssue as XlsxValidationIssue,
};

const WORKBOOK_PART: &str = "xl/workbook.xml";
const WORKBOOK_RELATIONSHIPS_PART: &str = "xl/_rels/workbook.xml.rels";
const WORKBOOK_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";

const PROFILE: OoxmlProfile = OoxmlProfile {
    format: "Excel",
    extension: "xlsx",
    main_part: WORKBOOK_PART,
    main_content_type: WORKBOOK_CONTENT_TYPE,
    required_parts: &[
        "[Content_Types].xml",
        "_rels/.rels",
        WORKBOOK_PART,
        WORKBOOK_RELATIONSHIPS_PART,
    ],
    forbidden_parts: &["xl/vbaProject.bin"],
    expected_root,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XlsxValidationReport {
    pub valid: bool,
    pub format: &'static str,
    pub source_kind: XlsxSourceKind,
    pub parts: usize,
    pub xml_parts: usize,
    pub relationship_parts: usize,
    pub worksheets: usize,
    pub cells: usize,
    pub formulas: usize,
    pub shared_strings: usize,
    pub cell_styles: usize,
    pub total_uncompressed_bytes: u64,
    pub error_count: usize,
    pub warning_count: usize,
    pub issues_truncated: bool,
    pub errors: Vec<XlsxValidationIssue>,
    pub warnings: Vec<XlsxValidationIssue>,
    pub checks: Vec<&'static str>,
    pub limitations: Vec<&'static str>,
}

impl XlsxValidationReport {
    fn from_summary(summary: OoxmlValidationSummary, counts: WorkbookCounts) -> Self {
        Self {
            valid: summary.valid,
            format: "xlsx",
            source_kind: summary.source_kind,
            parts: summary.parts,
            xml_parts: summary.xml_parts,
            relationship_parts: summary.relationship_parts,
            worksheets: counts.worksheets,
            cells: counts.cells,
            formulas: counts.formulas,
            shared_strings: counts.shared_strings,
            cell_styles: counts.cell_styles,
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

pub type XlsxValidationError = OoxmlValidationError;

#[derive(Debug, Default, Clone, Copy)]
pub struct XlsxValidator;

impl XlsxValidator {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn validate_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<XlsxValidationReport, XlsxValidationError> {
        let (mut state, package) = validate_common(path.as_ref(), &PROFILE)?;
        let counts = package
            .as_ref()
            .map_or_else(WorkbookCounts::default, |package| {
                validate_workbook(package, &mut state)
            });
        let summary = state.finish(
            "xlsx",
            vec![
                "workbook_sheet_references",
                "worksheet_cells",
                "shared_string_indices",
                "cell_style_indices",
            ],
            vec![
                "OOXML XML Schema conformance is not checked",
                "formula semantics, cached results and recalculation are not checked",
                "visual layout and Excel rendering are not checked",
            ],
        );
        Ok(XlsxValidationReport::from_summary(summary, counts))
    }
}

fn expected_root(name: &str) -> Option<&'static str> {
    if name == WORKBOOK_PART {
        Some("workbook")
    } else if name.starts_with("xl/worksheets/") && name.ends_with(".xml") {
        Some("worksheet")
    } else {
        match name {
            "xl/sharedStrings.xml" => Some("sst"),
            "xl/styles.xml" => Some("styleSheet"),
            "xl/calcChain.xml" => Some("calcChain"),
            "xl/connections.xml" => Some("connections"),
            _ if name.starts_with("xl/theme/") && name.ends_with(".xml") => Some("theme"),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct WorkbookCounts {
    worksheets: usize,
    cells: usize,
    formulas: usize,
    shared_strings: usize,
    cell_styles: usize,
}

#[derive(Debug)]
struct SheetReference {
    relationship_id: String,
}

fn validate_workbook(
    validated: &ValidatedOoxmlPackage,
    state: &mut ValidationState,
) -> WorkbookCounts {
    let Some(bytes) = validated
        .well_formed
        .contains(WORKBOOK_PART)
        .then(|| validated.package.xml(WORKBOOK_PART))
        .flatten()
    else {
        return WorkbookCounts::default();
    };
    let (worksheets, sheet_references) = parse_sheets(bytes, state);
    if worksheets == 0 {
        state.warning(
            "workbook_has_no_sheets",
            Some(WORKBOOK_PART),
            "workbook does not declare any worksheets",
        );
    }

    let workbook_relationships = validated
        .relationships
        .get(WORKBOOK_RELATIONSHIPS_PART)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let shared_strings_part = related_part(workbook_relationships, "/sharedStrings");
    let styles_part = related_part(workbook_relationships, "/styles");
    let shared_string_count = shared_strings_part
        .filter(|part| validated.well_formed.contains(*part))
        .and_then(|part| validated.package.xml(part))
        .map(count_shared_strings);
    let cell_style_count = styles_part
        .filter(|part| validated.well_formed.contains(*part))
        .and_then(|part| validated.package.xml(part))
        .map(count_cell_styles);

    let mut cells = 0;
    let mut formulas = 0;
    let mut worksheet_targets = BTreeSet::new();
    for sheet in sheet_references {
        let Some(relationship) = workbook_relationships
            .iter()
            .find(|relationship| relationship.id == sheet.relationship_id)
        else {
            state.error(
                "worksheet_relationship_missing",
                Some(WORKBOOK_PART),
                format!(
                    "sheet references unknown relationship {}",
                    sheet.relationship_id
                ),
            );
            continue;
        };
        if relationship.external || !relationship.relationship_type.ends_with("/worksheet") {
            state.error(
                "worksheet_relationship_type_invalid",
                Some(WORKBOOK_RELATIONSHIPS_PART),
                format!(
                    "relationship {} is not an internal worksheet relationship",
                    sheet.relationship_id
                ),
            );
            continue;
        }
        let Some(worksheet_part) = relationship.resolved_target.as_deref() else {
            continue;
        };
        if !worksheet_targets.insert(worksheet_part.to_owned()) {
            state.error(
                "duplicate_worksheet_target",
                Some(WORKBOOK_PART),
                format!("multiple sheets reference {worksheet_part}"),
            );
            continue;
        }
        if !validated.well_formed.contains(worksheet_part) {
            continue;
        }
        let Some(worksheet) = validated.package.xml(worksheet_part) else {
            continue;
        };
        let counts = validate_cells(
            worksheet_part,
            worksheet,
            shared_string_count,
            cell_style_count,
            state,
        );
        cells += counts.0;
        formulas += counts.1;
    }

    WorkbookCounts {
        worksheets,
        cells,
        formulas,
        shared_strings: shared_string_count.unwrap_or(0),
        cell_styles: cell_style_count.unwrap_or(0),
    }
}

fn parse_sheets(bytes: &[u8], state: &mut ValidationState) -> (usize, Vec<SheetReference>) {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut count = 0;
    let mut references = Vec::new();
    let mut sheet_ids = BTreeSet::new();
    let mut sheet_names = BTreeSet::new();
    let mut relationship_ids = BTreeSet::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element))
                if local_name(element.name().as_ref()) == "sheet" =>
            {
                count += 1;
                let Some(attributes) = decoded_attributes(&element) else {
                    buffer.clear();
                    continue;
                };
                match attributes.get("name") {
                    Some(name) => validate_sheet_name(name, &mut sheet_names, state),
                    None => state.error(
                        "sheet_name_missing",
                        Some(WORKBOOK_PART),
                        "sheet is missing its name",
                    ),
                }
                match attributes
                    .get("sheetId")
                    .and_then(|value| value.parse::<u32>().ok())
                    .filter(|value| *value > 0)
                {
                    Some(sheet_id) if sheet_ids.insert(sheet_id) => {}
                    Some(sheet_id) => state.error(
                        "duplicate_sheet_id",
                        Some(WORKBOOK_PART),
                        format!("duplicate sheetId {sheet_id}"),
                    ),
                    None => state.error(
                        "sheet_id_invalid",
                        Some(WORKBOOK_PART),
                        "sheetId must be a positive integer",
                    ),
                }
                let relationship_id = attributes
                    .iter()
                    .find(|(key, _)| key.ends_with(":id"))
                    .map(|(_, value)| value);
                match relationship_id.filter(|value| !value.is_empty()) {
                    Some(relationship_id) if relationship_ids.insert(relationship_id.clone()) => {
                        references.push(SheetReference {
                            relationship_id: relationship_id.clone(),
                        });
                    }
                    Some(relationship_id) => state.error(
                        "duplicate_sheet_relationship_id",
                        Some(WORKBOOK_PART),
                        format!("duplicate sheet relationship {relationship_id}"),
                    ),
                    None => state.error(
                        "sheet_relationship_id_missing",
                        Some(WORKBOOK_PART),
                        "sheet is missing r:id",
                    ),
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    (count, references)
}

fn validate_sheet_name(name: &str, names: &mut BTreeSet<String>, state: &mut ValidationState) {
    if name.is_empty() || name.chars().count() > 31 {
        state.error(
            "sheet_name_invalid",
            Some(WORKBOOK_PART),
            "sheet name must contain between 1 and 31 characters",
        );
    }
    if name.chars().any(|character| "[]:*?/\\".contains(character)) {
        state.error(
            "sheet_name_invalid",
            Some(WORKBOOK_PART),
            format!("sheet name contains a forbidden character: {name}"),
        );
    }
    if !names.insert(name.to_lowercase()) {
        state.error(
            "duplicate_sheet_name",
            Some(WORKBOOK_PART),
            format!("duplicate case-insensitive sheet name: {name}"),
        );
    }
}

fn related_part<'a>(relationships: &'a [Relationship], suffix: &str) -> Option<&'a str> {
    relationships
        .iter()
        .find(|relationship| {
            !relationship.external && relationship.relationship_type.ends_with(suffix)
        })
        .and_then(|relationship| relationship.resolved_target.as_deref())
}

fn count_shared_strings(bytes: &[u8]) -> usize {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut count = 0;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element))
                if local_name(element.name().as_ref()) == "si" =>
            {
                count += 1;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    count
}

fn count_cell_styles(bytes: &[u8]) -> usize {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut in_cell_xfs = false;
    let mut count = 0;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) if local_name(element.name().as_ref()) == "cellXfs" => {
                in_cell_xfs = true;
            }
            Ok(Event::End(element)) if local_name(element.name().as_ref()) == "cellXfs" => {
                in_cell_xfs = false;
            }
            Ok(Event::Start(element) | Event::Empty(element))
                if in_cell_xfs && local_name(element.name().as_ref()) == "xf" =>
            {
                count += 1;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    count
}

#[derive(Debug)]
struct Cell {
    reference: String,
    shared_string: bool,
    style: Option<String>,
    value: String,
}

fn validate_cells(
    worksheet_part: &str,
    bytes: &[u8],
    shared_string_count: Option<usize>,
    cell_style_count: Option<usize>,
    state: &mut ValidationState,
) -> (usize, usize) {
    let mut reader = Reader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut cell = None;
    let mut in_value = false;
    let mut cells = 0;
    let mut formulas = 0;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) if local_name(element.name().as_ref()) == "c" => {
                cells += 1;
                cell = Some(cell_from_element(&element, cells));
            }
            Ok(Event::Empty(element)) if local_name(element.name().as_ref()) == "c" => {
                cells += 1;
                validate_cell(
                    cell_from_element(&element, cells),
                    worksheet_part,
                    shared_string_count,
                    cell_style_count,
                    state,
                );
            }
            Ok(Event::Start(element) | Event::Empty(element))
                if cell.is_some() && local_name(element.name().as_ref()) == "f" =>
            {
                formulas += 1;
            }
            Ok(Event::Start(element))
                if cell.is_some() && local_name(element.name().as_ref()) == "v" =>
            {
                in_value = true;
            }
            Ok(Event::Text(text)) if in_value => {
                if let Some(cell) = cell.as_mut() {
                    cell.value.push_str(text.as_ref());
                }
            }
            Ok(Event::End(element)) if local_name(element.name().as_ref()) == "v" => {
                in_value = false;
            }
            Ok(Event::End(element)) if local_name(element.name().as_ref()) == "c" => {
                if let Some(cell) = cell.take() {
                    validate_cell(
                        cell,
                        worksheet_part,
                        shared_string_count,
                        cell_style_count,
                        state,
                    );
                }
                in_value = false;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buffer.clear();
    }
    (cells, formulas)
}

fn cell_from_element(element: &quick_xml::events::BytesStart<'_>, number: usize) -> Cell {
    let attributes = decoded_attributes(element).unwrap_or_default();
    Cell {
        reference: attributes
            .get("r")
            .cloned()
            .unwrap_or_else(|| format!("cell #{number}")),
        shared_string: attributes.get("t").is_some_and(|value| value == "s"),
        style: attributes.get("s").cloned(),
        value: String::new(),
    }
}

fn validate_cell(
    cell: Cell,
    worksheet_part: &str,
    shared_string_count: Option<usize>,
    cell_style_count: Option<usize>,
    state: &mut ValidationState,
) {
    if let Some(style) = cell.style {
        match (style.parse::<usize>(), cell_style_count) {
            (Ok(_), None) => state.error(
                "cell_styles_part_missing",
                Some(worksheet_part),
                format!(
                    "{} uses a style but the workbook has no styles part",
                    cell.reference
                ),
            ),
            (Ok(index), Some(count)) if index < count => {}
            (Ok(index), Some(count)) => state.error(
                "cell_style_index_out_of_range",
                Some(worksheet_part),
                format!(
                    "{} references style {index}, but only {count} cell styles exist",
                    cell.reference
                ),
            ),
            (Err(_), _) => state.error(
                "cell_style_index_invalid",
                Some(worksheet_part),
                format!("{} has a non-integer style index", cell.reference),
            ),
        }
    }
    if cell.shared_string {
        match (cell.value.trim().parse::<usize>(), shared_string_count) {
            (Ok(_), None) => state.error(
                "shared_strings_part_missing",
                Some(worksheet_part),
                format!(
                    "{} uses a shared string but the workbook has no shared strings part",
                    cell.reference
                ),
            ),
            (Ok(index), Some(count)) if index < count => {}
            (Ok(index), Some(count)) => state.error(
                "shared_string_index_out_of_range",
                Some(worksheet_part),
                format!(
                    "{} references shared string {index}, but only {count} entries exist",
                    cell.reference
                ),
            ),
            (Err(_), _) => state.error(
                "shared_string_index_invalid",
                Some(worksheet_part),
                format!("{} has an invalid shared string index", cell.reference),
            ),
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
    fn validates_unpacked_and_zipped_workbooks() {
        let directory = tempdir().expect("temporary directory");
        let package = directory.path().join("book");
        std::fs::create_dir(&package).expect("package directory");
        write_minimal_package(&package, "Sheet1", "0");

        let report = XlsxValidator::new()
            .validate_path(&package)
            .expect("validation should run");
        assert!(report.valid, "errors: {:?}", report.errors);
        assert_eq!(report.worksheets, 1);
        assert_eq!(report.cells, 2);
        assert_eq!(report.formulas, 1);

        let archive = directory.path().join("book.xlsx");
        zip_directory(&package, &archive);
        let report = XlsxValidator::new()
            .validate_path(&archive)
            .expect("validation should run");
        assert!(report.valid, "errors: {:?}", report.errors);
        assert_eq!(report.source_kind, XlsxSourceKind::Archive);
    }

    #[test]
    fn reports_sheet_and_shared_string_errors() {
        let directory = tempdir().expect("temporary directory");
        write_minimal_package(directory.path(), "Bad/Name", "4");
        let workbook = directory.path().join(WORKBOOK_PART);
        let xml = std::fs::read_to_string(&workbook).expect("workbook fixture");
        std::fs::write(
            workbook,
            xml.replace(
                "</sheets>",
                r#"<sheet name="bad/name" sheetId="1" r:id="rId1"/></sheets>"#,
            ),
        )
        .expect("overwrite fixture");

        let report = XlsxValidator::new()
            .validate_path(directory.path())
            .expect("validation should run");
        assert!(!report.valid);
        for code in [
            "sheet_name_invalid",
            "duplicate_sheet_name",
            "duplicate_sheet_id",
            "duplicate_sheet_relationship_id",
            "shared_string_index_out_of_range",
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
    const WORKSHEET_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
    const SHARED_STRINGS_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings";
    const STYLES_REL: &str =
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles";

    fn write_minimal_package(root: &Path, sheet_name: &str, shared_index: &str) {
        write_fixture(
            root,
            "[Content_Types].xml",
            &format!(
                r#"<Types><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="{WORKBOOK_CONTENT_TYPE}"/></Types>"#
            ),
        );
        write_fixture(
            root,
            "_rels/.rels",
            &relationships_xml(&[("rId1", OFFICE_DOCUMENT_REL, "xl/workbook.xml")]),
        );
        write_fixture(
            root,
            WORKBOOK_PART,
            &format!(
                r#"<workbook xmlns:r="urn:r"><sheets><sheet name="{sheet_name}" sheetId="1" r:id="rId1"/></sheets></workbook>"#
            ),
        );
        write_fixture(
            root,
            WORKBOOK_RELATIONSHIPS_PART,
            &relationships_xml(&[
                ("rId1", WORKSHEET_REL, "worksheets/sheet1.xml"),
                ("rId2", SHARED_STRINGS_REL, "sharedStrings.xml"),
                ("rId3", STYLES_REL, "styles.xml"),
            ]),
        );
        write_fixture(
            root,
            "xl/worksheets/sheet1.xml",
            &format!(
                r#"<worksheet><sheetData><row><c r="A1" t="s" s="0"><v>{shared_index}</v></c><c r="B1"><f>1+1</f><v>2</v></c></row></sheetData></worksheet>"#
            ),
        );
        write_fixture(
            root,
            "xl/sharedStrings.xml",
            "<sst><si><t>Hello</t></si></sst>",
        );
        write_fixture(
            root,
            "xl/styles.xml",
            "<styleSheet><cellXfs><xf/></cellXfs></styleSheet>",
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
        format!(r#"<Relationships>{entries}</Relationships>"#)
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
