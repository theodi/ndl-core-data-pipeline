from dagster import asset
import pandas as pd
from pathlib import Path
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

SOURCE_DATASET = "https://huggingface.co/datasets/hkir-dev/ndl-core-corpus/resolve/main/ndl_core_dataset.parquet"

# all-MiniLM-L6-v2 has a maximum sequence length of 256 tokens (~1000 characters)
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
FINAL_ASSET = TARGET_DIR / "ndl_core_dataset.parquet"
RAG_ASSET = TARGET_DIR / "ndl_core_rag_index.parquet"
FAISS_FILE = TARGET_DIR / "index.faiss"

model = SentenceTransformer('all-MiniLM-L6-v2')

@asset(
    group_name="rag"
)
def process_text_chunks():
    """
    Reads a Parquet file, processes text chunks, generates embeddings, and creates a FAISS index.
    """
    df = pd.read_parquet(FINAL_ASSET)
    text_rows = df[df['format'] == 'text']

    processed_chunks = process_chunks(text_rows)
    embeddings, ids = generate_embeddings(processed_chunks)

    save_to_parquet(processed_chunks, RAG_ASSET)
    create_faiss_index(embeddings, ids, str(FAISS_FILE))  # Convert Path to string

def process_chunks(df):
    """
    Splits text into chunks using RecursiveCharacterTextSplitter.
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", " ", ""],
        length_function=len,
    )

    chunks = []
    for _, row in df.iterrows():
        text = row['text']
        identifier = row['identifier']
        for chunk in text_splitter.split_text(text):
            chunks.append({"chunk": chunk,
                           "origin_identifier": identifier,
                           "language": row.get("language", "en"),
                           "source_dataset": SOURCE_DATASET})

    return pd.DataFrame(chunks)

def generate_embeddings(df):
    """
    Generates embeddings for text chunks using SentenceTransformer.
    """
    embeddings = model.encode(df['chunk'].tolist(), convert_to_numpy=True, show_progress_bar=True)
    ids = df.index.tolist()
    return embeddings, ids

def save_to_parquet(df, file_path):
    """
    Saves a DataFrame to a Parquet file.
    """
    df.to_parquet(file_path)

def create_faiss_index(embeddings, ids, file_path):
    """
    Creates and saves a FAISS index.

    Args:
        embeddings (list or np.ndarray): 2D array of embeddings.
        ids (list or np.ndarray): 1D array of unique IDs.
        file_path (str): Path to save the FAISS index.

    Raises:
        ValueError: If dimensions or data types are invalid.
    """
    # Ensure embeddings are a NumPy array
    embeddings_np = np.array(embeddings, dtype='float32')
    ids_np = np.array(ids, dtype=np.int64).flatten()  # Ensure ids are a 1D NumPy array

    # Validate dimensions
    if len(embeddings_np.shape) != 2:
        raise ValueError("Embeddings must be a 2D array.")
    if len(ids_np.shape) != 1:
        raise ValueError("IDs must be a 1D array.")
    if embeddings_np.shape[0] != ids_np.shape[0]:
        raise ValueError("The number of embeddings must match the number of IDs.")

    # Create FAISS index
    dimension = embeddings_np.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index_with_ids = faiss.IndexIDMap2(index)  # Use IndexIDMap2 for better compatibility

    # Debugging: Print shapes and types
    print(f"Embeddings shape: {embeddings_np.shape}, dtype: {embeddings_np.dtype}")
    print(f"IDs shape: {ids_np.shape}, dtype: {ids_np.dtype}")

    index_with_ids.add_with_ids(embeddings_np, ids_np)

    # Save the FAISS index
    faiss.write_index(index_with_ids, file_path)

    print(f"FAISS index saved to {file_path}")
