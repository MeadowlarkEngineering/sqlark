from unittest import mock
from pytest import mark
from dataclasses import dataclass
from query_builder import Select
from query_builder.response_formatters import (
    decompose_dict_response_formatter,
    object_response_formatter,
    RelationFormatter
)

@dataclass
class TableName:
    column_name: str | None = None
    column2: str | None = None

@dataclass
class Comments:
    id: int | None = None
    post_id: int | None = None
    author_id: int | None = None
    comment: str | None = None

@dataclass
class Posts:
    id: int | None = None
    title: str | None = None
    author_id: int | None = None

@dataclass
class Authors:
    id: int | None = None
    name: str | None = None

def dataclass_for_table(table_name, pg_config):
    if table_name == 'table_name':
        return TableName
    if table_name == 'comments':
        return Comments
    if table_name == 'posts':
        return Posts
    if table_name == 'authors':
        return Authors

@mark.parametrize('response, expected', [
    ([{'table_name.column_name': 'value'}], [{'table_name': {'column_name': 'value'}}]),
    ([{'table_name.column_name': 'value', 'table_name.column2': 'value2', 'table_name2.column_name2': 'value2'}], [{'table_name': {'column_name': 'value', 'column2': 'value2'}, 'table_name2': {'column_name2': 'value2'}}]),
    ([{'column_name': 'value'}], [{'column_name': 'value'}]),
    ([{'column_name': 'value', 'table_name.column_name': 'value2'}], [{'column_name': 'value', 'table_name': {'column_name': 'value2'}}]),
    ([{'column_name': 'value', 'table_name.column_name': 'value2', 'table_name2.column_name2': 'value3'}], [{'column_name': 'value', 'table_name': {'column_name': 'value2'}, 'table_name2': {'column_name2': 'value3'}}]),
])
def test_decompose_dict_response_formatter(response, expected):
    assert decompose_dict_response_formatter(response, None) == expected


@mark.parametrize('response', [
    ([{'table_name.column_name': 'value'}])
])
@mock.patch('query_builder.response_formatters.dataclass_for_table', return_value=TableName)
def test_object_response_formatter(patch, response):
    formatted_response = object_response_formatter(response, None, Select("table_name"))
    assert formatted_response[0].column_name == 'value'

@mock.patch('query_builder.response_formatters.dataclass_for_table', side_effect=dataclass_for_table)
def test_relation_formatter(_):
    relation_formatter = RelationFormatter()
    relation_formatter.set_relation("posts.comments", "comments")
    relation_formatter.set_relation("comments.author", "authors", relationship_type="one")

    result_set = [
        {"posts": {'id': 1, 'title': 'Post 1'}, 
         "comments": {'id': 1, 'post_id': 1, 'author_id': 1, 'comment': 'Comment 1'},
         "authors": {'id': 1, 'name': 'Author 1'}},
        {"posts": {'id': 1, 'title': 'Post 1'}, 
         "comments": {'id': 2, 'post_id': 1, 'author_id': 1, 'comment': 'Comment 2'},
         "authors": {'id': 1, 'name': 'Author 1'}},
        {"posts": {'id': 2, 'title': 'Post 2'}, 
         "comments": {'id': 3, 'post_id': 2, 'author_id': 2, 'comment': 'Comment 3'},
         "authors": {'id': 3, 'name': 'Author 2'}},

    ]

    formatted_response = relation_formatter.format(result_set, None, Select("posts"))
    print(formatted_response)
    assert len(formatted_response) == 2
    assert len(formatted_response[0].comments) == 2
    assert len(formatted_response[1].comments) == 1
    assert formatted_response[0].id == 1
    assert formatted_response[0].title == 'Post 1'
    assert formatted_response[1].id == 2
    assert formatted_response[1].title == 'Post 2'

    assert formatted_response[0].comments[0].comment == 'Comment 1'
    assert formatted_response[0].comments[1].comment == 'Comment 2'
    assert formatted_response[0].comments[0].author.name == 'Author 1'
    assert formatted_response[0].comments[1].author.name == 'Author 1'
    assert formatted_response[1].comments[0].comment == 'Comment 3'

@mock.patch('query_builder.response_formatters.dataclass_for_table', side_effect=dataclass_for_table)
def test_object_response_formatter(_):
    response = [
        {'table_name.column_name': 'value1'},
        {'table_name.column_name': 'value2'},
    ]
    formatted_response = object_response_formatter(response, None, Select("table_name"))
    assert formatted_response[0].column_name == 'value1'
    assert formatted_response[1].column_name == 'value2'

@mock.patch('query_builder.response_formatters.dataclass_for_table', side_effect=dataclass_for_table)
def test_object_response_formatter_response_to_insert(_):
    """
    Tests that the object response formatter can handle an insert response
    which does not have a table name in the response
    """
    response = [{'column_name': 'value'}]
    formatted_response = object_response_formatter(response, None, Select("table_name"))
    assert formatted_response[0].column_name == 'value'

