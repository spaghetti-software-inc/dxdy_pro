
from pathlib import Path
import ast
import json

from openai import OpenAI

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field

from dxdy.settings import Settings

import rich

class SqlQuery(BaseModel):
    query : str

def get_gpt_query(schema_ddl_json, views_ddl_json, user_query):
    client = OpenAI()
    
    system_prompt = f"You are a helpful data analyst, writing correct SQL queries for a simple DuckDB database. Here is the DDL for the database schema: {schema_ddl_json}."
    user_prompt = f"Please write a SQL query that returns the following information: {user_query}. Please ensure the query is runnable against the exact databse schema."

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=SqlQuery,
    )

    output = completion.choices[0].message.parsed

    return output

 
def get_str_from_node(node):
    """
    Attempt to extract a string from an AST node.
    Handles simple constant strings and joined strings (f-strings).
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    elif isinstance(node, ast.Str):  # For Python <3.8
        return node.s
    elif isinstance(node, ast.JoinedStr):
        # For f-strings, concatenate the constant parts.
        parts = []
        for value in node.values:
            if isinstance(value, ast.Str):
                parts.append(value.s)
            elif isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            else:
                parts.append("{...}")  # Placeholder for non-constant parts.
        return "".join(parts)
    return None

class SQLExtractor(ast.NodeVisitor):
    def __init__(self):
        self.sql_statements = []  # List of tuples: (identifier, sql_string)

    def visit_Assign(self, node):
        # Check if the assigned value is a string that looks like SQL.
        sql_string = get_str_from_node(node.value)
        if sql_string is not None:
            # Look at each target in the assignment.
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.endswith('_sql'):
                    self.sql_statements.append((target.id, sql_string))
        self.generic_visit(node)

    def visit_Call(self, node):
        # Check for method calls named "execute"
        if isinstance(node.func, ast.Attribute) and node.func.attr == "execute":
            if node.args:
                sql_string = get_str_from_node(node.args[0])
                if sql_string:
                    self.sql_statements.append(("execute", sql_string))
        self.generic_visit(node)

def extract_sql_from_file(filename):
    # Read the target Python script.
    with open(filename, "r") as f:
        source = f.read()

    # Parse the source code into an AST.
    tree = ast.parse(source, filename)

    # Create an extractor and visit the AST.
    extractor = SQLExtractor()
    extractor.visit(tree)

    res = []
    # Print out all extracted SQL statement strings.
    for ident, sql in extractor.sql_statements:
        #sql = sql.strip()
        res.append({"ident": ident, "sql": sql})

    return res

class SqlProgramer:
    
    def __init__(self):
        dir = Settings().get_dxdy_user_dir()
        
        schema_file_path = dir / "schema.py"
        schema_ddl = extract_sql_from_file(schema_file_path)
        self.schema_ddl_json = json.dumps(schema_ddl, indent=4)
        
        views_file_path = dir / "views.py"
        views_ddl = extract_sql_from_file(views_file_path)
        self.views_ddl_json = json.dumps(views_ddl, indent=4)
        
        

    
    def generate_sql(self, user_query: str) -> str:
        sql_query = get_gpt_query(self.schema_ddl_json, self.views_ddl_json, user_query)
        
        return sql_query.query
    
    