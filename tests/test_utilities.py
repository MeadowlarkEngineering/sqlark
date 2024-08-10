from pytest import mark 
from query_builder import utilities
from unittest import TestCase

@mark.parametrize('row, expected', [
    ({'table_name.column_name': 'value'}, {'table_name': {'column_name': 'value'}}),
    ({'table_name.column_name': 'value', 'table_name.column2': 'value2', 'table_name2.column_name2': 'value2'}, {'table_name': {'column_name': 'value', 'column2': 'value2'}, 'table_name2': {'column_name2': 'value2'}}),
    ({'column_name': 'value'}, {'column_name': 'value'}),
    ({'column_name': 'value', 'table_name.column_name': 'value2'}, {'column_name': 'value', 'table_name': {'column_name': 'value2'}}),
    ({'column_name': 'value', 'table_name.column_name': 'value2', 'table_name2.column_name2': 'value3'}, {'column_name': 'value', 'table_name': {'column_name': 'value2'}, 'table_name2': {'column_name2': 'value3'}}),
])
def test_decompose_row(row, expected):
    TestCase().assertDictEqual(utilities.decompose_row(row), expected)