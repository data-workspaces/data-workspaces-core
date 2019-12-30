"""
Utilities for printing and formatting
"""
from typing import Dict, Any, NamedTuple, List, Optional
import click


class ColSpec(NamedTuple):
    precision: Optional[int] = None
    width: Optional[int] = None # if spec, specifies exact width. Does not include
                                # one spec padding each side
    alignment: str = 'left'


def pad_left(s, width):
    if len(s)==width:
        return s
    if len(s)<width:
        return (' '*(width-len(s))) + s
    else:
        wrapped = []
        while len(s)>=width:
            wrapped.append(s[0:width])
            s = s[width:]
        if len(s)>0:
            wrapped.append(pad_left(s, width))
        return '\n'.join(wrapped)

def pad_right(s, width):
    if len(s)==width:
        return s
    if len(s)<width:
        return s + (' '*(width-len(s)))
    else:
        wrapped = []
        while len(s)>=width:
            wrapped.append(s[0:width])
            s = s[width:]
        if len(s)>0:
            wrapped.append(pad_right(s, width))
        return '\n'.join(wrapped)

class FormattedColumns(NamedTuple):
    nitems: int
    headers: List[str]
    columns: Dict[str, List[str]]
    widths: List[int]

def format_columns(columns:Dict[str,Any], precision=2, spec:Dict[str,ColSpec]={})\
    -> FormattedColumns:
    str_cols = {} # type: Dict[str,List[str]]
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
        assert nitems_for_col==nitems
        cspec = spec.get(col, None)
        if cspec is not None and cspec.precision is not None:
            col_precision = cspec.precision
        else:
            col_precision = precision
        max_value_width = 0
        strvalues = []
        all_numeric = True
        # find the maxium width, determine if numeric, and do rounding
        for v in values:
            if isinstance(v, float):
                v = round(v, col_precision)
            if not isinstance(v, (int, float)):
                all_numeric = False
            s = str(v)
            if len(s)>max_value_width:
                max_value_width = len(s)
            strvalues.append(s)
        if cspec is not None and cspec.width is not None:
            colwidth = cspec.width
        else:
            colwidth = max(len(col), max_value_width)
        headers.append(pad_right(col, colwidth)) # headers are always left aligned
        widths.append(colwidth)
        if (cspec is not None and cspec.alignment=='right') or \
           (cspec is None and all_numeric):
            pad_fn = pad_left
        else:
            pad_fn = pad_right
        # second pass does padding and line breaks
        str_cols[col] =[pad_fn(strvalue, colwidth) for strvalue in strvalues]
    return FormattedColumns(nitems, headers, str_cols, widths)


def format_row(columns:List[str], widths:List[int]) -> str:
    """Format one row of the table. This may go into multiple lines if
    any of the columns were wrapped."""
    row = ''
    had_values = True
    lineno = 0
    while had_values:
        had_values = False
        line = ''
        for (i, c) in enumerate(columns):
            collines = c.split('\n')
            if len(collines)>lineno:
                had_values = True
                line += '| '+ collines[lineno] + ' '
            else:
                line += '|' + ' '*(2+widths[i])
        if had_values:
            row += line + '|\n'
            lineno += 1
    return row[0:-1]

def row_generator(cols:FormattedColumns, nl=False):
    #header row
    for line in format_row(cols.headers, cols.widths).split('\n'):
        if nl:
            yield line + '\n'
        else:
            yield line
    # divider row
    div = '|' + '|'.join(['_'*(width+2) for width in cols.widths]) + '|'
    if nl:
        yield div + '\n'
    else:
        yield div
    # values
    for i in range (cols.nitems):
        for line in format_row([values[i] for values in cols.columns.values()], cols.widths).split('\n'):
            if nl:
                yield line + '\n'
            else:
                yield line

def print_columns(columns:Dict[str,Any], precision=2, spec:Dict[str,ColSpec]={},
                  paginate:bool=True):
    cols = format_columns(columns, precision, spec)
    if paginate:
        click.echo_via_pager(row_generator(cols, nl=True))
    else:
        for line in row_generator(cols):
            click.echo(line)
