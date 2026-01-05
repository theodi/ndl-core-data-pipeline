import faiss
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from pathlib import Path


CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
RAG_ASSET = TARGET_DIR / "ndl_core_rag_index.parquet"
FAISS_FILE = TARGET_DIR / "index.faiss"
CHUNK_OVERLAP = 100  # Define the overlap size for merging chunks

def search(query: str, n: int = 15):
    """
    Perform a RAG search using a FAISS index and a parquet file.

    Args:
        query (str): The input string to search for.
        n (int): The number of top results to return. Default is 15.

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
    # Filter results adaptively
    distances, indices = filter_results_adaptive(distances, indices, sensitivity=2.5)

    # Load the parquet file
    df = pd.read_parquet(RAG_ASSET)

    # Prepare the results
    results = []
    for i, idx in enumerate(indices[0]):
        if idx == -1:  # Skip invalid indices
            continue
        record = df.iloc[idx]
        origin_identifier = record["origin_identifier"]

        # Initialize the merged chunk with the current chunk
        merged_chunk = record["chunk"]

        # Check the preceding chunk
        if idx > 0:  # Ensure index is not out of range
            prev_record = df.iloc[idx - 1]
            if prev_record["origin_identifier"] == origin_identifier:
                overlap = min(CHUNK_OVERLAP, len(prev_record["chunk"]))
                merged_chunk = prev_record["chunk"][:-overlap] + merged_chunk

        # Check the succeeding chunk
        if idx < len(df) - 1:  # Ensure index is not out of range
            next_record = df.iloc[idx + 1]
            if next_record["origin_identifier"] == origin_identifier:
                overlap = min(CHUNK_OVERLAP, len(next_record["chunk"]))
                merged_chunk = merged_chunk + next_record["chunk"][overlap:]

        # Append the result
        results.append({
            "chunk": merged_chunk,
            "origin_identifier": origin_identifier,
            "distance": distances[0][i]
        })

    return results


def filter_results_adaptive(D, I, sensitivity=2.5, min_step=0.05):
    """
    Filters FAISS results based on the distribution of distances (Elbow Method).

    Args:
        D (np.array): 2D array of distances from FAISS (1 x k).
        I (np.array): 2D array of indices from FAISS (1 x k).
        sensitivity (float): Multiplier for the median jump. Higher = stricter (cuts less).
        min_step (float): Minimum absolute jump required to trigger a cut (prevents cutting on noise).

    Returns:
        tuple: (filtered_distances, filtered_indices) as 1D lists/arrays.
    """
    # FAISS returns 2D arrays [[1, 2, 3]], so we access the first row
    distances = D[0]
    indices = I[0]

    print(distances)

    # Edge case: If results are empty or single item, return as is
    if len(distances) < 2:
        return D, I

    # Calculate velocity/diffs
    diffs = np.diff(distances)

    # Calculate baseline step (median)
    median_step = np.median(diffs)
    if median_step == 0: median_step = 1e-6

    # Find the cut index
    cut_index = len(distances)  # Default: keep all

    for i, step in enumerate(diffs):
        if step > (median_step * sensitivity) and step > min_step:
            cut_index = i + 1
            break

    # Slicing the 1D data
    filtered_d = distances[:cut_index]
    filtered_i = indices[:cut_index]

    return np.array([filtered_d]), np.array([filtered_i])

if __name__ == "__main__":
    # query = "An enduring covenant between the people of the United Kingdom,"
    # query = "How much can I save with Tax-Free Childcare."
    # query = "What conditions must a site satisfy to be granted excluded disposal site status?"
    query = "What percentage of all working days lost in 2022 were due to respiratory conditions?"
    # query = "In which year between 2011 and 2023 were excess deaths highest for those aged 65 and over?"
    # query = "Who is Gen Kitchen."
    results = search(query, n=15)
    for res in results:
        print(f"Identifier: {res['origin_identifier']}, Distance: {res['distance']}\nChunk: {res['chunk']}\n")