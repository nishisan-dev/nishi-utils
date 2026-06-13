"""Command-line helpers for exporting NGRR series files."""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom

from .errors import NgrrdFormatError
from .models import ConsolidationFunction
from .reader import NgrrdReader


def to_xml(data: object, metadata: dict[str, object]) -> str:
    """Serialize exported metadata and archive data into a small XML document."""

    root = ET.Element("ngrrd")
    meta_elem = ET.SubElement(root, "metadata")
    for key, value in metadata.items():
        item = ET.SubElement(meta_elem, key)
        if isinstance(value, list):
            for child in value:
                ET.SubElement(item, "item").text = str(child)
        else:
            item.text = str(value)

    data_elem = ET.SubElement(root, "data")
    if isinstance(data, list):
        for row in data:
            row_elem = ET.SubElement(data_elem, "row")
            for key, value in row.items():
                ET.SubElement(row_elem, key).text = str(value)
    elif isinstance(data, dict):
        for ds_name, points in data.items():
            ds_elem = ET.SubElement(data_elem, "datasource", name=str(ds_name))
            for point in points:
                point_elem = ET.SubElement(ds_elem, "point")
                for key, value in point.items():
                    ET.SubElement(point_elem, key).text = str(value)

    xml_bytes = ET.tostring(root, encoding="utf-8")
    return minidom.parseString(xml_bytes).toprettyxml(indent="  ")


def geometry_to_xml(geometry: dict[str, object]) -> str:
    """Serialize a :meth:`NgrrdReader.describe_geometry` payload into XML."""

    root = ET.Element("ngrrd-geometry")
    root.set("file", str(geometry.get("file", "")))
    root.set("last_update_ms", str(geometry.get("last_update_ms", "")))

    header = geometry.get("header", {})
    header_elem = ET.SubElement(root, "header")
    if isinstance(header, dict):
        for key, value in header.items():
            ET.SubElement(header_elem, key).text = str(value)

    columns_elem = ET.SubElement(root, "columns")
    for column in geometry.get("columns", []):
        attrs = {key: str(value) for key, value in column.items()}
        ET.SubElement(columns_elem, "column", attrs)

    archives_elem = ET.SubElement(root, "archives")
    for archive in geometry.get("archives", []):
        attrs = {key: str(value) for key, value in archive.items()}
        ET.SubElement(archives_elem, "archive", attrs)

    xml_bytes = ET.tostring(root, encoding="utf-8")
    return minidom.parseString(xml_bytes).toprettyxml(indent="  ")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export NGRR geometry or archive data to JSON or XML")
    parser.add_argument("file", type=Path, help="Path to the .ngrr file")
    parser.add_argument("--archive", help="Archive/RRA name to dump; omit for geometry only")
    parser.add_argument(
        "--geometry",
        action="store_true",
        help="Dump the structural geometry only (no ring data); takes precedence over --archive",
    )
    parser.add_argument(
        "--cf",
        choices=[cf.name for cf in ConsolidationFunction],
        help="Consolidation function when the RRA has multiple archives",
    )
    parser.add_argument("--format", choices=["json", "xml"], default="json", help="Output format")
    parser.add_argument("--rows", action="store_true", help="Export row-oriented data")
    parser.add_argument("--output", type=Path, help="Output file path; defaults to stdout")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    geometry_mode = args.geometry or args.archive is None

    try:
        cf = ConsolidationFunction[args.cf] if args.cf else None
        with NgrrdReader(args.file) as reader:
            if geometry_mode:
                geometry = reader.describe_geometry()
                output = (
                    json.dumps(geometry, indent=2, allow_nan=True)
                    if args.format == "json"
                    else geometry_to_xml(geometry)
                )
            else:
                metadata = reader.get_metadata()
                data = (
                    reader.read_archive_rows(args.archive, cf)
                    if args.rows
                    else reader.read_archive_as_dict(args.archive, cf)
                )
                output = (
                    json.dumps({"metadata": metadata, "data": data}, indent=2, allow_nan=True)
                    if args.format == "json"
                    else to_xml(data, metadata)
                )

        if args.output:
            args.output.write_text(output, encoding="utf-8")
        else:
            print(output)
        return 0
    except (NgrrdFormatError, OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
