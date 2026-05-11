from __future__ import annotations

from typing import List, Literal, Optional

import dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

input_text = """
The Great Gatsby by F. Scott Fitzgerald, published 1925, genre: Fiction
Dune by Frank Herbert, published 1965, genre: Science Fiction
1984 by George Orwell, published 1949, genre: Dystopian
"""

class fields(BaseModel):
    title: str = Field(description="The title of the book")
    author: str = Field(description="The author of the book")
    published_year: int = Field(description="The year the book was published")

class rows_test(BaseModel):
    rows_test: List[fields] = Field(description="A list of book entries extracted from the input text")

_SYSTEM_PROMPT = """\
You are a helpful assistant that extracts structured data from unstructured text.
Given an input text describing books, you will return a JSON object with a list of rows.
"""

def _get_book_chain():
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _SYSTEM_PROMPT),
            ("human", "Extract the book data from the following text:\n\n{input_text}"),
        ]
    )

    llm = ChatAnthropic(model="claude-haiku-4-5-20251001", temperature=0)

    structure_output = llm.with_structured_output(rows_test)

    return prompt | structure_output

def invoke_book_chain(input_text: str) -> rows_test:
    chain = _get_book_chain()
    return chain.invoke(
        {
            "input_text": input_text
            }
        )

if __name__ == "__main__":
    dotenv.load_dotenv()
    result = invoke_book_chain(input_text)