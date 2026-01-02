import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer
from pathlib import Path


CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
RAG_ASSET = TARGET_DIR / "ndl_core_rag_index.parquet"
FAISS_FILE = TARGET_DIR / "index.faiss"

def search(query: str, n: int = 5):
    """
    Perform a RAG search using a FAISS index and a parquet file.

    Args:
        query (str): The input string to search for.
        n (int): The number of top results to return. Default is 5.

    Returns:
        list[dict]: A list of dictionaries containing "chunk", "origin_identifier", and "distance".
    """
    # Load the embedding model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Generate embedding for the query
    query_embedding = model.encode([query])

    # Load the FAISS index
    faiss_index = faiss.read_index(str(FAISS_FILE))

    # Search the FAISS index
    distances, indices = faiss_index.search(query_embedding, n)

    # Load the parquet file
    df = pd.read_parquet(RAG_ASSET)

    # Prepare the results
    results = []
    for i, idx in enumerate(indices[0]):
        if idx == -1:  # Skip invalid indices
            continue
        record = df.iloc[idx]
        results.append({
            "chunk": record["chunk"],
            "origin_identifier": record["origin_identifier"],
            "distance": distances[0][i]
        })

    return results

if __name__ == "__main__":
    query = "What are the result of the International Climate Finance (ICF) programme."
    results = search(query, n=5)
    for res in results:
        print(f"Identifier: {res['origin_identifier']}, Distance: {res['distance']}\nChunk: {res['chunk']}\n")