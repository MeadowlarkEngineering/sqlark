import datetime
from dataclasses import asdict
from decimal import Decimal
from typing import Optional, List
from unittest import mock
from pytest import mark
from dataclasses import dataclass
from query_builder import Select
from query_builder.utilities import ColumnDefinition
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

@dataclass
class Organizations:
    id: int | None = None
    created_at: datetime.datetime | None = None
    name: str | None = None

@dataclass
class Subscriptions:
    id: int | None = None
    organization_id: int | None = None
    expiration: datetime.datetime | None = None
    created_at: datetime.datetime | None = None

def dataclass_for_table(table_name, pg_config):
    if table_name == 'table_name':
        return TableName
    if table_name == 'comments':
        return Comments
    if table_name == 'posts':
        return Posts
    if table_name == 'authors':
        return Authors
    if table_name == 'organizations':
        return Organizations
    if table_name == 'subscriptions':
        return Subscriptions
    
def column_defs_side_effect(table_name, pg_config):
    if table_name == 'posts':
        return [
            ColumnDefinition(name='id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='title', data_type='text', is_nullable=True, default=None),
        ]
    if table_name == 'comments':
        return [
            ColumnDefinition(name='id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='post_id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='author_id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='comment', data_type='text', is_nullable=True, default=None),
        ]
    if table_name == 'authors':
        return [
            ColumnDefinition(name='id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='name', data_type='text', is_nullable=True, default=None),
        ]
    if table_name == 'organizations':
        return [
            ColumnDefinition(name='id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='created_at', data_type='timestamp without time zone', is_nullable=True, default=None),
            ColumnDefinition(name='name', data_type='text', is_nullable=True, default=None),
        ]
    if table_name == 'subscriptions':
        return [
            ColumnDefinition(name='id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='organization_id', data_type='integer', is_nullable=True, default=None),
            ColumnDefinition(name='expiration', data_type='timestamp without time zone', is_nullable=True, default=None),
            ColumnDefinition(name='created_at', data_type='timestamp without time zone', is_nullable=True, default=None),
        ]

@mark.parametrize('response, expected', [
    ([{'table_name.column_name': 'value'}], [{'table_name': {'column_name': 'value'}}]),
    ([{'table_name.column_name': 'value', 'table_name.column2': 'value2', 'table_name2.column_name2': 'value2'}], [{'table_name': {'column_name': 'value', 'column2': 'value2'}, 'table_name2': {'column_name2': 'value2'}}]),
    ([{'column_name': 'value'}], [{'column_name': 'value'}]),
    ([{'column_name': 'value', 'table_name.column_name': 'value2'}], [{'column_name': 'value', 'table_name': {'column_name': 'value2'}}]),
    ([{'column_name': 'value', 'table_name.column_name': 'value2', 'table_name2.column_name2': 'value3'}], [{'column_name': 'value', 'table_name': {'column_name': 'value2'}, 'table_name2': {'column_name2': 'value3'}}]),
    ([{'table_name.column_name': 'value', 'table_name2.column_name': None}], [{'table_name': {'column_name': 'value'}, 'table_name2': {'column_name': None}}]),
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

@mock.patch('query_builder.response_formatters.get_column_definitions', side_effect=column_defs_side_effect)
def test_relation_formatter_dataclass_for_table(pg_connection):
    relation_formatter = RelationFormatter()
    relation_formatter.set_relation("posts.comments", "comments")
    relation_formatter.set_relation("comments.author", "authors", relationship_type="one")

    dc = relation_formatter.dataclass_for_table("posts", None)
    assert dc.__name__ == 'Posts'
    
    fields = dc.__dataclass_fields__
    assert 'id' in fields and fields['id'].type == Optional[int]
    assert 'title' in fields and fields['title'].type == Optional[str]
    assert 'comments' in fields and fields['comments'].type.__origin__ == list
    assert fields['comments'].type.__args__[0].__name__ == 'Comments'

    dc = relation_formatter.dataclass_for_table("comments", None)
    assert dc.__name__ == 'Comments'
    fields = dc.__dataclass_fields__
    assert 'id' in fields and fields['id'].type == Optional[int] 
    assert 'post_id' in fields and fields['post_id'].type == Optional[int]
    assert 'author_id' in fields and fields['author_id'].type == Optional[int]
    assert 'comment' in fields and fields['comment'].type == Optional[str]
    assert fields['author'].type.__name__ == 'Authors'
    
    dc = relation_formatter.dataclass_for_table("authors", None)
    assert dc.__name__ == 'Authors'
    fields = dc.__dataclass_fields__
    assert 'id' in fields and fields['id'].type == Optional[int]
    assert 'name' in fields and fields['name'].type == Optional[str]

@mock.patch('query_builder.response_formatters.get_column_definitions', side_effect=column_defs_side_effect)
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

    assert asdict(formatted_response[0]) == {
        'id': 1,
        'title': 'Post 1',
        'comments': [
            {'id': 1, 'post_id': 1, 'author_id': 1, 'comment': 'Comment 1', 'author': {'id': 1, 'name': 'Author 1'}},
            {'id': 2, 'post_id': 1, 'author_id': 1, 'comment': 'Comment 2', 'author': {'id': 1, 'name': 'Author 1'}}
        ]
    }

@mock.patch('query_builder.response_formatters.get_column_definitions', side_effect=column_defs_side_effect)
def test_relation_formatter2(_):

    result_set = [
        {'organizations': {
            'id': 14769, 
            'created_at': datetime.datetime(2024, 12, 27, 19, 9, 45, 613040), 
            'name': 'org1'}, 
         'subscriptions': {
             'id': 15056, 
             'organization_id': 14769, 
             'expiration': datetime.datetime(2025, 1, 1, 19, 9, 45, 616841), 
             'created_at': datetime.datetime(2024, 12, 27, 19, 9, 45, 619882)}}, 
        {'organizations': {
            'id': 14770, 
            'created_at': datetime.datetime(2024, 12, 27, 19, 9, 45, 616233), 
            'name': 'org2'}, 
         'subscriptions': {
             'id': 15057, 
             'organization_id': 14770, 
             'expiration': datetime.datetime(2024, 12, 22, 19, 9, 45, 620992), 
             'created_at': datetime.datetime(2024, 12, 27, 19, 9, 45, 624174)}}]

    relation_formatter = RelationFormatter()
    relation_formatter.set_relation("organizations.subscriptions", "subscriptions")
    formatted_response = relation_formatter.format(result_set, None, Select("organizations"))
    assert len(formatted_response) == 2
    assert formatted_response[0].id == 14769
    assert formatted_response[0].name == 'org1'
    assert formatted_response[0].subscriptions[0].id == 15056
    assert formatted_response[0].subscriptions[0].organization_id == 14769
    assert formatted_response[0].subscriptions[0].expiration == datetime.datetime(2025, 1, 1, 19, 9, 45, 616841)
    assert formatted_response[0].subscriptions[0].created_at == datetime.datetime(2024, 12, 27, 19, 9, 45, 619882)

    assert asdict(formatted_response[0]) == {
        'id': 14769,
        'created_at': datetime.datetime(2024, 12, 27, 19, 9, 45, 613040),
        'name': 'org1',
        'subscriptions': [
            {'id': 15056, 'organization_id': 14769, 'expiration': datetime.datetime(2025, 1, 1, 19, 9, 45, 616841), 'created_at': datetime.datetime(2024, 12, 27, 19, 9, 45, 619882)}
        ]
    }

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

@mock.patch('query_builder.response_formatters.get_column_definitions', side_effect=column_defs_side_effect)
def test_empty_relation(_):
    relation_formatter = RelationFormatter().set_relation("posts.comments", "comments")
    result_set = [
        {"posts": {'id': 1, 'title': 'Post 1'}, "comments": {'id': None, 'post_id': None, 'author_id': None, 'comment': None}},
    ]

    formatted_response = relation_formatter.format(result_set, None, Select("posts"))
    assert len(formatted_response) == 1
    assert formatted_response[0].id == 1
    assert formatted_response[0].title == 'Post 1'
    assert formatted_response[0].comments == []