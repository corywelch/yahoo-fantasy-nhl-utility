from pathlib import Path
from typing import Dict, List
import pandas as pd
from openpyxl.utils import get_column_letter

def _write_sheet(writer, df: pd.DataFrame, name: str) -> None:
    if df is None or df.empty:
        pd.DataFrame().to_excel(writer, sheet_name=name, index=False)
        return
    df.to_excel(writer, sheet_name=name, index=False)
    ws = writer.sheets[name]
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"
    for i, col in enumerate(df.columns, start=1):
        values = [str(col)] + df[col].astype(str).tolist()
        max_len = max(len(v) for v in values) if values else 10
        ws.column_dimensions[get_column_letter(i)].width = min(max(10, max_len + 2), 60)

def league_pack_to_excel(league_info: Dict, teams: List[Dict], scoring: Dict, xlsx_path: Path) -> None:
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    league_df = pd.DataFrame([league_info])
    teams_df = pd.DataFrame(teams)
    cats_df = pd.DataFrame(scoring.get("stat_categories", []))
    mods_df = pd.DataFrame(scoring.get("stat_modifiers", []))
    roster_df = pd.DataFrame(scoring.get("roster_positions", []))
    tb_df = pd.DataFrame([{"rank": i + 1, "rule": r} for i, r in enumerate(scoring.get("tiebreakers", []))])
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        _write_sheet(writer, league_df, "League")
        _write_sheet(writer, teams_df, "Teams")
        _write_sheet(writer, cats_df, "ScoringCategories")
        _write_sheet(writer, mods_df, "StatModifiers")
        _write_sheet(writer, roster_df, "RosterPositions")
        _write_sheet(writer, tb_df, "TieBreakers")
