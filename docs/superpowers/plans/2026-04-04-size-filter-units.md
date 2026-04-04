# Size Filter Unit Dropdowns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add KB/MB/GB unit dropdowns next to the Min size and Max size fields so users don't have to type raw byte values.

**Architecture:** All changes are in `ui_components.py`. Two `QComboBox` widgets (one per size field) are added in `_create_advanced_filters_row`. Value extraction in `get_search_params` and validation in `_validate_inputs` are updated to multiply the typed integer by the selected unit factor. No downstream changes (engine, controller, models) are needed since the values passed remain plain `int | None` bytes.

**Tech Stack:** PySide6 (`QComboBox`, `QLineEdit`, `QHBoxLayout`)

---

### Task 1: Add unit combo boxes in `_create_advanced_filters_row`

**Files:**
- Modify: `search_script/ui_components.py:221-232`

- [ ] **Step 1: Replace Min size widgets**

In `_create_advanced_filters_row` replace:

```python
row.addWidget(QLabel("Min size (bytes):"))
self.min_size_entry = QLineEdit()
self.min_size_entry.setPlaceholderText("e.g. 1048576")
self.min_size_entry.setFixedWidth(100)
row.addWidget(self.min_size_entry)
row.addSpacing(15)
```

with:

```python
row.addWidget(QLabel("Min size:"))
self.min_size_entry = QLineEdit()
self.min_size_entry.setPlaceholderText("e.g. 10")
self.min_size_entry.setFixedWidth(80)
row.addWidget(self.min_size_entry)
self.min_size_unit = QComboBox()
self.min_size_unit.addItems(["KB", "MB", "GB"])
self.min_size_unit.setCurrentText("MB")
row.addWidget(self.min_size_unit)
row.addSpacing(15)
```

- [ ] **Step 2: Replace Max size widgets**

Replace:

```python
row.addWidget(QLabel("Max size (bytes):"))
self.max_size_entry = QLineEdit()
self.max_size_entry.setPlaceholderText("no limit")
self.max_size_entry.setFixedWidth(100)
row.addWidget(self.max_size_entry)
row.addSpacing(15)
```

with:

```python
row.addWidget(QLabel("Max size:"))
self.max_size_entry = QLineEdit()
self.max_size_entry.setPlaceholderText("no limit")
self.max_size_entry.setFixedWidth(80)
row.addWidget(self.max_size_entry)
self.max_size_unit = QComboBox()
self.max_size_unit.addItems(["KB", "MB", "GB"])
self.max_size_unit.setCurrentText("MB")
row.addWidget(self.max_size_unit)
row.addSpacing(15)
```

- [ ] **Step 3: Run lint**

```bash
uv run ruff check search_script/ui_components.py --fix
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add search_script/ui_components.py
git commit -m "feat: add KB/MB/GB unit combos next to size filter fields"
```

---

### Task 2: Update `get_search_params` to convert units to bytes

**Files:**
- Modify: `search_script/ui_components.py` — `get_search_params` method (~line 412)

The current code:
```python
"min_size": self._parse_optional_int(self.min_size_entry.text()),
"max_size": self._parse_optional_int(self.max_size_entry.text()),
```

- [ ] **Step 1: Add a helper to convert a field's value to bytes**

Add this private method to `SearchUI` (place it near `_parse_optional_int`):

```python
def _parse_size_bytes(self, text: str, unit_combo: QComboBox) -> int | None:
    """Return size in bytes, or None if text is empty."""
    raw = text.strip()
    if not raw:
        return None
    unit_map = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
    return int(raw) * unit_map[unit_combo.currentText()]
```

- [ ] **Step 2: Update `get_search_params` to use the helper**

Replace:
```python
"min_size": self._parse_optional_int(self.min_size_entry.text()),
"max_size": self._parse_optional_int(self.max_size_entry.text()),
```

with:
```python
"min_size": self._parse_size_bytes(self.min_size_entry.text(), self.min_size_unit),
"max_size": self._parse_size_bytes(self.max_size_entry.text(), self.max_size_unit),
```

- [ ] **Step 3: Run lint and type check**

```bash
uv run ruff check search_script/ui_components.py --fix
uv run basedpyright --level error search_script/ui_components.py
```

Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add search_script/ui_components.py
git commit -m "feat: convert size filter values to bytes using selected unit"
```

---

### Task 3: Update `_validate_inputs` to apply unit conversion before comparison

**Files:**
- Modify: `search_script/ui_components.py` — `_validate_inputs` method (~line 454)

The current validation reads the raw integer text for min/max size, then compares the raw values. After the unit change, the raw text is a small number (e.g. `10`) so integer format validation still works, but the min > max comparison must use byte-converted values.

- [ ] **Step 1: Keep the integer-format check as-is**

The loop at ~line 454 still validates that the text (if present) is a non-negative integer — no change needed there. Verify it reads:

```python
for label, widget in (
    ("Max depth", self.depth_entry),
    ("Min size", self.min_size_entry),
    ("Max size", self.max_size_entry),
):
    raw = widget.text().strip()
    if not raw:
        continue
    if not raw.isdigit():
        QMessageBox.warning(self, "Input Error", f"{label} must be a non-negative integer.")
        return False
```

This is unchanged — leave it as-is.

- [ ] **Step 2: Update the min > max comparison to use byte values**

Replace:
```python
min_size = self._parse_optional_int(self.min_size_entry.text())
max_size = self._parse_optional_int(self.max_size_entry.text())
if min_size is not None and max_size is not None and min_size > max_size:
    QMessageBox.warning(self, "Input Error", "Min size cannot be greater than max size.")
    return False
```

with:
```python
min_size = self._parse_size_bytes(self.min_size_entry.text(), self.min_size_unit)
max_size = self._parse_size_bytes(self.max_size_entry.text(), self.max_size_unit)
if min_size is not None and max_size is not None and min_size > max_size:
    QMessageBox.warning(self, "Input Error", "Min size cannot be greater than max size.")
    return False
```

- [ ] **Step 3: Run lint and type check**

```bash
uv run ruff check search_script/ui_components.py --fix
uv run basedpyright --level error search_script/ui_components.py
```

Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add search_script/ui_components.py
git commit -m "fix: apply unit conversion before min/max size comparison in validation"
```

---

### Task 4: Update "Large Files (>10MB)" preset

**Files:**
- Modify: `search_script/ui_components.py` — `_apply_preset` method (~line 605)

- [ ] **Step 1: Update the preset handler**

Replace:
```python
if preset == "Large Files (>10MB)":
    self.min_size_entry.setText("10485760")
else:
    self.min_size_entry.clear()
```

with:
```python
if preset == "Large Files (>10MB)":
    self.min_size_entry.setText("10")
    self.min_size_unit.setCurrentText("MB")
else:
    self.min_size_entry.clear()
```

- [ ] **Step 2: Run lint**

```bash
uv run ruff check search_script/ui_components.py --fix
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add search_script/ui_components.py
git commit -m "fix: update Large Files preset to use 10 MB with unit combo"
```

---

### Task 5: Smoke-test the full feature

- [ ] **Step 1: Run the app**

```bash
uv run file-search
```

- [ ] **Step 2: Verify UI**

Open Advanced Filters. Confirm:
- "Min size:" and "Max size:" labels (no "(bytes)")
- Both fields have "e.g. 10" / "no limit" placeholder text
- Both dropdowns show KB / MB / GB, defaulting to MB

- [ ] **Step 3: Verify "Large Files" preset**

Select the "Large Files (>10MB)" preset from the preset dropdown.
Confirm Min size field shows `10` and unit combo shows `MB`.

- [ ] **Step 4: Verify cross-unit validation**

Set Min size = `1`, unit = `GB`. Set Max size = `500`, unit = `MB`.
Click Search. Confirm error: "Min size cannot be greater than max size."

- [ ] **Step 5: Verify a successful search**

Set Min size = `1`, unit = `KB`. Clear Max size.
Run a search on a directory. Confirm results only include files ≥ 1 024 bytes.

- [ ] **Step 6: Run full lint and type check**

```bash
uv run ruff check . --fix
uv run basedpyright --level error
```

Expected: 0 errors.

- [ ] **Step 7: Run tests**

```bash
uv run pytest
```

Expected: all pass.

- [ ] **Step 8: Final commit (if any auto-fixes were applied)**

```bash
git add -u
git commit -m "chore: post-feature lint fixes"
```

Only commit if there are actual changes from the ruff auto-fix.
