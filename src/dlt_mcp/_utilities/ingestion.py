"""Utilities to ingest `dlt` documentation and code to power the MCP search tools."""

import zipfile
import tempfile
from pathlib import Path
from typing import Callable, Iterator

import chonkie.logger
import lancedb
import numpy as np
import requests
import model2vec
from lancedb.embeddings import TextEmbeddingFunction, get_registry, register
from lancedb.pydantic import LanceModel, Vector
from chonkie import RecursiveChunker, BaseChunker, CodeChunker
from requests.exceptions import HTTPError

# TODO figure out a mechanism to retrieve the user's dlt version
# and set it at server init.
DLT_VERSION = "1.18.1"
DLT_DOCS_RELATIVE_PATH = ".dlt/mcp"
DLT_DOCS_TABLE_NAME = "pages"
DLT_DOCS_CHUNKS_TABLE_NAME = "page_chunks"
DLT_CODE_CHUNKS_TABLE_NAME = "code_chunks"
_DOCS_PATH_SEGMENT = "docs/website/docs/"
_CODE_PATH_SEGMENT = "dlt/"
_GITHUB_REPOSITORY_URL = "https://github.com/dlt-hub/dlt"
_GITHUB_REPOSITORY_API_URL = "https://api.github.com/repos/dlt-hub/dlt"


chonkie.logger.disable()


@register("model2vec")
class Model2VecEmbeddings(TextEmbeddingFunction):
    name: str = "minishlab/potion-base-32M"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._embedding_model = None

    def generate_embeddings(self, texts: list[str]) -> list[np.ndarray]:
        vectors = self._load_model().encode(
            list(texts),
            max_length=None,
        )
        return vectors.tolist()

    def ndims(self) -> int:
        return 512

    def _load_model(self):
        if self._embedding_model is None:
            self._embedding_model = model2vec.StaticModel.from_pretrained(self.name)
        return self._embedding_model


_lancedb_embedding_model = get_registry().get("model2vec").create()


class PageChunkSchema(LanceModel):
    id: str
    text: str = _lancedb_embedding_model.SourceField()
    token_count: int
    file_path: str
    start_index: int
    end_index: int
    chunk_index: int
    vector: Vector(_lancedb_embedding_model.ndims()) = (  # type: ignore[invalid-type-form]
        _lancedb_embedding_model.VectorField()
    )


# TODO add a boolean for public-facing API? this could be derived from `griffe`
class CodeChunkSchema(LanceModel):
    id: str
    text: str = _lancedb_embedding_model.SourceField()
    token_count: int
    file_path: str
    start_index: int
    end_index: int
    chunk_index: int
    vector: Vector(_lancedb_embedding_model.ndims()) = (  # type: ignore[invalid-type-form]
        _lancedb_embedding_model.VectorField()
    )


def _get_lancedb_path(dlt_version: str) -> Path:
    db_name = f"docs_{dlt_version.replace('.', '-')}.lancedb"
    return Path.home() / DLT_DOCS_RELATIVE_PATH / db_name


def _is_docs_file(file_name: str) -> bool:
    """Filter out files that aren't source documentation files
    Returns `False` if the file is not a documentation file, `True` otherwise.
    """
    if _DOCS_PATH_SEGMENT not in file_name:
        return False

    if not file_name.endswith(".md"):
        return False

    return True


def _is_code_file(file_name: str) -> bool:
    """Filter out files that aren't source code files
    Returns `False` if the file is not a code file, `True` otherwise.
    """
    if _CODE_PATH_SEGMENT not in file_name:
        return False

    if not file_name.endswith(".py"):
        return False

    return True


def _iterate_over_zipped_files(
    zip_content: bytes,
    *,
    predicate: Callable[[str], bool],
) -> Iterator[tuple[zipfile.ZipFile, str]]:
    """Unzip to a temporary directory and iterate over files"""
    with tempfile.TemporaryDirectory() as tmp_dir:  # type: ignore
        tmp_zip_path = Path(tmp_dir, "dlt-release-archive").with_suffix(".zip")
        tmp_zip_path.write_bytes(zip_content)

        with zipfile.ZipFile(tmp_zip_path, "r") as zip_ref:
            for file_name in zip_ref.namelist():
                if predicate(file_name) is False:
                    continue

                yield zip_ref, file_name


# TODO tune chunker
def docs_chunker() -> RecursiveChunker:
    """Instantiate the docs chunker"""
    chunker = RecursiveChunker.from_recipe(name="markdown", lang="en")
    chunker.chunk_size = 2048
    return chunker


# TODO tune chunker
def code_chunker() -> CodeChunker:
    """Instantiate the code chunker"""
    chunker = CodeChunker(
        language="python", tokenizer="character", chunk_size=2048, include_nodes=False
    )
    return chunker


def release_zipped_archive(
    dlt_version: str,
    repo: str = _GITHUB_REPOSITORY_URL,
) -> bytes:
    """Retrieve the zip archive for a package release on GitHub"""
    zip_url = f"{repo}/archive/refs/tags/{dlt_version}.zip"
    try:
        zip_response = requests.get(zip_url)
        zip_response.raise_for_status()
    except HTTPError:
        raise

    return zip_response.content


def docs_pages(release_zipped_archive: bytes) -> list[dict[str, str]]:
    """Retrieve the raw docs pages from the release's source code."""
    pages = []
    for zip_ref, file_name in _iterate_over_zipped_files(
        release_zipped_archive,
        predicate=_is_docs_file,
    ):
        with zip_ref.open(file_name) as file_obj:
            page = {
                "text": file_obj.read().decode(errors="replace"),
                "file_path": file_name.split(_DOCS_PATH_SEGMENT)[-1],
            }
            pages.append(page)

    return pages


def _page_chunks(
    docs_page: dict[str, str], docs_chunker: BaseChunker
) -> list[dict[str, str]]:
    """Process a single page into chunks"""
    chunks = []
    # TODO try different chunking strategies; chonkie supports adding overlap
    # and using semantic chunking
    for idx, chunked in enumerate(docs_chunker.chunk(docs_page["text"])):
        chunk = chunked.to_dict()
        del chunk["embedding"]
        del chunk["context"]
        chunk.update(
            file_path=docs_page["file_path"],
            chunk_index=idx,
        )
        chunks.append(chunk)
    return chunks


def docs_chunks(
    docs_pages: list[dict[str, str]], docs_chunker: BaseChunker
) -> list[dict[str, str]]:
    """Retrieve raw docs pages from the GitHub repository"""
    chunks = []
    for page in docs_pages:
        chunks.extend(_page_chunks(page, docs_chunker))
    return chunks


def code_files(release_zipped_archive: bytes) -> list[dict[str, str]]:
    """Retrieve the raw docs pages from the release's source code."""
    files = []
    for zip_ref, file_name in _iterate_over_zipped_files(
        release_zipped_archive,
        predicate=_is_code_file,
    ):
        with zip_ref.open(file_name) as file_obj:
            code_file = {
                "text": file_obj.read().decode(errors="replace"),
                "file_path": "dlt/" + file_name.split(_CODE_PATH_SEGMENT)[-1],
            }
            files.append(code_file)

    return files


def _code_chunks(
    code_file: dict[str, str], code_chunker: BaseChunker
) -> list[dict[str, str]]:
    """Process a single code file into chunks"""
    chunks = []
    for idx, chunked in enumerate(code_chunker.chunk(code_file["text"])):
        chunk = chunked.to_dict()
        del chunk["embedding"]
        del chunk["context"]
        chunk.update(file_path=code_file["file_path"], chunk_index=idx)
        chunks.append(chunk)
    return chunks


def code_chunks(
    code_files: list[dict[str, str]], code_chunker: BaseChunker
) -> list[dict[str, str]]:
    """Retrieve raw code files from the GitHub repository"""
    chunks = []
    for code_file in code_files:
        chunks.extend(_code_chunks(code_file, code_chunker))
    return chunks


def db_con(dlt_version: str) -> lancedb.DBConnection:
    """Create a connection to the LanceDB database for the given DLT version"""
    return lancedb.connect(_get_lancedb_path(dlt_version))


def page_chunks_table(
    db_con: lancedb.DBConnection,
    docs_chunks: list[dict[str, str]],
) -> lancedb.Table:
    """Maybe create a table for docs page chunks and load data.

    LanceDB is responsible for generating the embeddings based on the `schema` passed.
    """
    table = db_con.create_table(
        DLT_DOCS_CHUNKS_TABLE_NAME,
        data=docs_chunks,
        exist_ok=True,
        schema=PageChunkSchema,
    )
    table.create_fts_index("text")
    return table


def code_chunks_table(
    db_con: lancedb.DBConnection,
    code_chunks: list[dict[str, str]],
) -> lancedb.Table:
    """Maybe create a table for code chunks and load data.

    LanceDB is responsible for generating the embeddings based on the `schema` passed.
    """
    table = db_con.create_table(
        DLT_CODE_CHUNKS_TABLE_NAME,
        data=code_chunks,
        exist_ok=True,
        schema=CodeChunkSchema,
    )
    table.create_fts_index("text")
    return table


def _ingest_docs(dlt_version: str, repo: str = _GITHUB_REPOSITORY_URL):
    _db_con = db_con(dlt_version)
    _release_zipped_archive = release_zipped_archive(dlt_version, repo)

    _docs_chunker = docs_chunker()
    _docs_pages = docs_pages(_release_zipped_archive)
    _docs_chunks = docs_chunks(_docs_pages, _docs_chunker)
    _page_chunks_table = page_chunks_table(_db_con, _docs_chunks)
    return _page_chunks_table


def _ingest_code(dlt_version: str, repo: str = _GITHUB_REPOSITORY_URL):
    _db_con = db_con(dlt_version)
    _release_zipped_archive = release_zipped_archive(dlt_version, repo)

    _code_chunker = code_chunker()
    _code_files = code_files(_release_zipped_archive)
    _code_chunks = code_chunks(_code_files, _code_chunker)
    _code_chunks_table = code_chunks_table(_db_con, _code_chunks)
    return _code_chunks_table


def _maybe_ingest_docs_and_code(dlt_version: str):
    """Ingest docs and code if the local LanceDB database is not found
    of if tables are missing."""
    local_db_path = _get_lancedb_path(dlt_version)
    if not local_db_path.exists():
        _ingest_docs(dlt_version)
        _ingest_code(dlt_version)
        return

    all_tables = list(local_db_path.iterdir())
    if not any(
        f"{DLT_DOCS_CHUNKS_TABLE_NAME}.lance" == table.name for table in all_tables
    ):
        _ingest_docs(dlt_version)

    if not any(
        f"{DLT_CODE_CHUNKS_TABLE_NAME}.lance" == table.name for table in all_tables
    ):
        _ingest_code(dlt_version)


if __name__ == "__main__":
    # TODO this can be optimized
    _ingest_docs(DLT_VERSION)
    _ingest_code(DLT_VERSION)
