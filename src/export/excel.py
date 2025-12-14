"""Excel export utilities for Yahoo Fantasy NHL data."""

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from openpyxl.utils import get_column_letter

def _write_sheet(writer, dataframe: pd.DataFrame, sheet_name: str) -> None:
    """Write DataFrame to Excel sheet with formatting.

    Args:
        writer: pandas ExcelWriter object
        dataframe: DataFrame to write
        sheet_name: Name of Excel sheet
    """
    if dataframe is None or dataframe.empty:
        pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
        return

    dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]

    # Set up auto-filter and freeze panes
    worksheet.auto_filter.ref = worksheet.dimensions
    worksheet.freeze_panes = "A2"

    # Auto-size columns with reasonable limits
    for column_index, column_name in enumerate(dataframe.columns, start=1):
        values = [str(column_name)] + dataframe[column_name].astype(str).tolist()
        max_length = max(len(value) for value in values) if values else 10
        column_width = min(max(10, max_length + 2), 60)
        worksheet.column_dimensions[get_column_letter(column_index)].width = column_width

def league_pack_to_excel(
    league_info: Dict,
    teams: List[Dict],
    scoring: Dict,
    excel_path: Path
) -> None:
    """Export league data to Excel workbook.

    Creates a multi-sheet Excel workbook containing league information,
    teams, and scoring settings in separate sheets.

    Args:
        league_info: League metadata dictionary
        teams: List of team dictionaries
        scoring: Scoring settings dictionary
        excel_path: Destination Excel file path
    """
    # Ensure output directory exists
    excel_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert data to DataFrames
    league_dataframe = pd.DataFrame([league_info])
    teams_dataframe = pd.DataFrame(teams)
    categories_dataframe = pd.DataFrame(scoring.get("stat_categories", []))
    modifiers_dataframe = pd.DataFrame(scoring.get("stat_modifiers", []))
    roster_dataframe = pd.DataFrame(scoring.get("roster_positions", []))

    # Format tiebreakers as ranked list
    tiebreakers_data = [
        {"rank": index + 1, "rule": rule}
        for index, rule in enumerate(scoring.get("tiebreakers", []))
    ]
    tiebreakers_dataframe = pd.DataFrame(tiebreakers_data)

    # Write all sheets to Excel workbook
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        _write_sheet(writer, league_dataframe, "League")
        _write_sheet(writer, teams_dataframe, "Teams")
        _write_sheet(writer, categories_dataframe, "ScoringCategories")
        _write_sheet(writer, modifiers_dataframe, "StatModifiers")
        _write_sheet(writer, roster_dataframe, "RosterPositions")
        _write_sheet(writer, tiebreakers_dataframe, "TieBreakers")
