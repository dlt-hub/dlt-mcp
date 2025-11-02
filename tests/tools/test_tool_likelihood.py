import pandas as pd
import asyncio
import pytest
from sentence_transformers import SentenceTransformer
from dlt_mcp.server import create_server


@pytest.fixture
def query_tool_data() -> pd.DataFrame:
    return pd.read_csv("tests/resources/query_tool_dataset.csv")


def _tools_and_descriptions() -> dict[str, str]:
    mcp_server = create_server()

    tools = asyncio.run(mcp_server.get_tools())
    return {tool_name: value.description or "" for tool_name, value in tools.items()}


def get_embedding(model: SentenceTransformer, sentence: list[str]):
    return model.encode_document(sentence)


def query_description_similarities(
    row: pd.Series, tool_descriptions: dict[str, str], model: SentenceTransformer
) -> pd.Series:
    query = row["query"]

    query_embeddings = get_embedding(model, [query])  # type: ignore
    description_embeddings = get_embedding(model, list(tool_descriptions.values()))

    similarities = model.similarity(query_embeddings, description_embeddings)[0]  # type: ignore
    for tool_name, similarity_score in zip(tool_descriptions.keys(), similarities):
        row[tool_name] = float(similarity_score)
    return row


# TODO: mark this as slow test, since it's loading a full dataset which at the moment is small
def test_query_likelihood_by_description_and_tool_name(query_tool_data):
    assert not query_tool_data.empty

    model = SentenceTransformer("all-MiniLM-L6-v2")
    tool_descriptions = _tools_and_descriptions()

    similarities = query_tool_data.apply(
        query_description_similarities, args=[tool_descriptions, model], axis=1
    )
    similarities["closest_tool"] = similarities[list(tool_descriptions.keys())].idxmax(
        axis=1
    )

    match_rate = sum(similarities["closest_tool"] == similarities["tool"]) / len(
        similarities.index
    )

    # TODO: store the similarities data as an asset with each build
    assert match_rate == 1.0
