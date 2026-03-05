import json

notebook_path = r"d:\DATA\2025-11-28_MSPR-1_2\Good-Air\etl\trans_hisotrique_aqi.ipynb"

with open(notebook_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find and remove the cell with `add_lags_pandas` id
cells_to_keep = [cell for cell in nb["cells"] if cell.get("id") != "add_lags_pandas"]

if len(cells_to_keep) < len(nb["cells"]):
    nb["cells"] = cells_to_keep
    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1)
    print("Notebook updated successfully: Lag cell removed.")
else:
    print("Lag cell not found.")
