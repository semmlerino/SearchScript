# Size Filter Unit Selection

**Date:** 2026-04-04
**Status:** Approved

## Summary

Add KB/MB/GB unit dropdowns next to the Min size and Max size input fields so users don't have to type raw byte values.

## Motivation

Currently the size filter fields expect raw bytes (e.g. `10485760` for 10 MB), which is error-prone and unfriendly. Adding a unit selector makes the filter intuitive and consistent with how users think about file sizes.

## Design

### UI Changes (`ui_components.py`)

Replace each labeled size field with a value + unit pair:

```
Min size: [____] [MB▼]    Max size: [____] [MB▼]
```

- Labels: "Min size:" and "Max size:" (remove "(bytes)" suffix)
- Each `QLineEdit` keeps `setFixedWidth(100)`; placeholder text becomes `"e.g. 10"`
- Each `QComboBox` has three options: `KB`, `MB`, `GB`; default selection is `MB`
- Widget order in the row: label → text field → unit combo

### Value Extraction

`get_search_params()` converts the user's input to bytes before returning:

```python
KB, MB, GB = 1024, 1024**2, 1024**3
unit_map = {"KB": KB, "MB": MB, "GB": GB}
# For each size field:
raw = self.min_size_entry.text().strip()
unit = self.min_size_unit.currentText()
min_size = int(raw) * unit_map[unit] if raw else None
```

The `min_size` / `max_size` values passed to `SearchController` remain plain `int | None` bytes — no downstream changes required.

### Preset Update

The "Large Files (>10MB)" preset sets:
- `min_size_entry` → `"10"`
- `min_size_unit` → `"MB"`
- `max_size_entry` → cleared
- `max_size_unit` → `"MB"` (default, no change needed)

### Validation

Existing validation is preserved:

1. Non-integer input → error message, search blocked
2. `min_size_bytes > max_size_bytes` → error message ("Min size cannot be greater than max size")

Unit conversion happens before the comparison, so cross-unit comparisons (e.g. `1 GB` vs `500 MB`) work correctly.

## Scope

**Only `ui_components.py` changes.** No modifications to:
- `SearchEngine`
- `SearchController`
- `SearchParams` data model
- `config.py`
- Tests (no UI tests exist)

## Implementation Notes

- Add two new instance attributes: `self.min_size_unit` and `self.max_size_unit` (both `QComboBox`)
- Update `_validate_inputs()` to extract bytes using the unit multiplier before the min > max check
- The size fields have no config persistence (no save/load state), so no persistence changes are needed
