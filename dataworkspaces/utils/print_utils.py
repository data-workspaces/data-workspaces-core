"""
Utilities for printing and formatting
"""
from typing import Dict, Any, NamedTuple, List, Optional
import click


class ColSpec(NamedTuple):
    precision: Optional[int] = None
    width: Optional[int] = None  # if spec, specifies exact width. Does not include
    # one spec padding each side
    truncate: bool = False  # If True and width is specified, truncate column
    # rather than wrapping it.
    alignment: str = "auto"


def _truncate(s, width):
    truncated = False
    if "\n" in s:
        s = s[0, s.index("\n")]  # tuncate at the first newline
        truncated = True
    if len(s) > (width - 2):
        s = s[0 : width - 2]
        truncated = True
    if truncated:
        s += ".."
    return s


def pad_left(s, width, truncate=False):
    if truncate:
        s = _truncate(s, width)
    if "\n" in s:
        return "\n".join([pad_left(fragment, width) for fragment in s.split("\n")])
    if len(s) == width:
        return s
    if len(s) < width:
        return (" " * (width - len(s))) + s
    else:
        wrapped = []
        while len(s) >= width:
            wrapped.append(s[0:width])
            s = s[width:]
        if len(s) > 0:
            wrapped.append(pad_left(s, width))
        return "\n".join(wrapped)


def pad_right(s, width, truncate=False):
    if truncate:
        s = _truncate(s, width)
    if "\n" in s:
        return "\n".join([pad_right(fragment, width) for fragment in s.split("\n")])
    if len(s) == width:
        return s
    if len(s) < width:
        return s + (" " * (width - len(s)))
    else:
        wrapped = []
        while len(s) >= width:
            wrapped.append(s[0:width])
            s = s[width:]
        if len(s) > 0:
            wrapped.append(pad_right(s, width))
        return "\n".join(wrapped)


class FormattedColumns(NamedTuple):
    nitems: int
    headers: List[str]
    columns: Dict[str, List[str]]
    widths: List[int]


def format_columns(
    columns: Dict[str, Any], precision=-1, null_value: str = "None", spec: Dict[str, ColSpec] = {}
) -> FormattedColumns:
    """Format and pad the individual columns.

    A precision of -1 means to adjust based on whether absolute value is less than one.
    """
    str_cols = {}  # type: Dict[str,List[str]]
    # start an iteration to get the length of the first column
    nitems = 0
    for values in columns.values():
        nitems = len(values)
        break
    headers = []
    widths = []
    # now go through all columns and compute string representations
    for (col, values) in columns.items():
        nitems_for_col = len(values)
        assert nitems_for_col == nitems
        cspec = spec.get(col, None)
        if cspec is not None and cspec.precision is not None:
            col_precision = cspec.precision
        else:
            col_precision = precision
        max_value_width = 0
        strvalues = []
        all_numeric = True
        truncate = False
        # find the maxium width, determine if numeric, and do rounding
        for v in values:
            if isinstance(v, float):
                if col_precision != -1:
                    v = round(v, col_precision)
                elif v > -1.0 and v < 1.0:
                    v = round(v, 3)
                else:
                    v = round(v, 1)
            if not isinstance(v, (int, float)) and v is not None:
                all_numeric = False
            if v is not None and v != "":
                s = str(v)
            else:
                s = null_value
            if "\n" in s:
                # special case if there are line breaks
                fragments = s.split("\n")
                len_s = max([len(fragment) for fragment in fragments])
            else:
                len_s = len(s)
            if len_s > max_value_width:
                max_value_width = len_s
            strvalues.append(s)
        width_needed = max(len(col), max_value_width)
        if cspec is not None and cspec.width is not None:
            if width_needed >= cspec.width:
                colwidth = cspec.width
                if cspec.truncate:
                    assert cspec.width > 3, "Column too short to truncate"
                    truncate = True
            else:
                # if possible, we make the column narrower than
                # specified.
                colwidth = width_needed
        else:
            colwidth = width_needed
        headers.append(pad_right(col, colwidth))  # headers are always left aligned
        widths.append(colwidth)
        if cspec is not None and cspec.alignment != "auto":
            if cspec.alignment == "right":
                pad_fn = pad_left
            else:
                pad_fn = pad_right
        elif all_numeric:
            pad_fn = pad_left
        else:
            pad_fn = pad_right
        # second pass does padding and line breaks
        str_cols[col] = [pad_fn(strvalue, colwidth, truncate) for strvalue in strvalues]
    return FormattedColumns(nitems, headers, str_cols, widths)


def format_row(columns: List[str], widths: List[int]) -> str:
    """Format one row of the table. This may go into multiple lines if
    any of the columns were wrapped."""
    row = ""
    had_values = True
    lineno = 0
    while had_values:
        had_values = False
        line = ""
        for (i, c) in enumerate(columns):
            collines = c.split("\n")
            if len(collines) > lineno:
                had_values = True
                line += "| " + collines[lineno] + " "
            else:
                line += "|" + " " * (2 + widths[i])
        if had_values:
            row += line + "|\n"
            lineno += 1
    return row[0:-1]


def row_generator(cols: FormattedColumns, title: Optional[str] = None, nl=False):
    if title is not None:
        for line in title.split("\n"):
            if nl:
                yield line + "\n"
            else:
                yield line
    # header row
    for line in format_row(cols.headers, cols.widths).split("\n"):
        if nl:
            yield line + "\n"
        else:
            yield line
    # divider row
    div = "|" + "|".join(["_" * (width + 2) for width in cols.widths]) + "|"
    if nl:
        yield div + "\n"
    else:
        yield div
    # values
    for i in range(cols.nitems):
        for line in format_row([values[i] for values in cols.columns.values()], cols.widths).split(
            "\n"
        ):
            if nl:
                yield line + "\n"
            else:
                yield line


def print_columns(
    columns: Dict[str, Any],
    precision=-1,
    null_value: str = "None",
    spec: Dict[str, ColSpec] = {},
    paginate: bool = True,
    title: Optional[str] = None,
) -> None:
    """Print the columns as a table.

    A precision of -1 means to adjust based on whether absolute value is less than one.
    """
    cols = format_columns(columns, precision, null_value, spec)
    if paginate:
        click.echo_via_pager(row_generator(cols, title=title, nl=True))
    else:
        for line in row_generator(cols, title=title):
            click.echo(line)
