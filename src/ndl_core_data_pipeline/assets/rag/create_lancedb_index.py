import os
import shutil
from dagster import asset
import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer
import lancedb

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
SOURCE_PARQUET = TARGET_DIR / "ndl_core_dataset.parquet"
LANCEDB_PATH = TARGET_DIR / "lancedb_search_index"

MODEL_NAME = 'all-MiniLM-L6-v2'
model = SentenceTransformer(MODEL_NAME)


def generate_search_text(row: pd.Series) -> str:
    """
    Generate a search text combining title, description, and first 500 characters of text.

    Args:
        row: A pandas Series representing a single record.

    Returns:
        A combined search text string.
    """
    parts = []

    title = row.get('title', '')
    if title and pd.notna(title):
        parts.append(str(title).strip())

    description = row.get('description', '')
    if description and pd.notna(description):
        parts.append(str(description).strip())

    text = row.get('text', '')
    if text and pd.notna(text):
        text_preview = str(text)[:500].strip()
        if text_preview:
            parts.append(text_preview)

    return " ".join(parts)


def generate_embeddings(texts: list[str]) -> list[list[float]]:
    """
    Generate embeddings for a list of texts using SentenceTransformer.

    Args:
        texts: List of text strings to embed.

    Returns:
        List of embedding vectors.
    """
    embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=True)
    return embeddings.tolist()


@asset(group_name="rag")
def create_lancedb_index():
    """
    Creates a LanceDB vector search index from the NDL core dataset.

    This asset:
    1. Reads the ndl_core_dataset.parquet file
    2. Generates search_text for each record (title + description + text[:500])
    3. Creates embeddings for all search texts
    4. Stores all data with embeddings in LanceDB
    5. Creates a vector search index on the embeddings
    """
    print(f"Reading source parquet from: {SOURCE_PARQUET}")
    df = pd.read_parquet(SOURCE_PARQUET)
    print(f"Loaded {len(df)} records")

    # Generate search text for each record
    print("Generating search text for each record...")
    df['search_text'] = df.apply(generate_search_text, axis=1)

    # Filter out records with empty search text
    df_valid = df[df['search_text'].str.len() > 0].copy()
    print(f"Records with valid search text: {len(df_valid)}")

    if len(df_valid) == 0:
        raise ValueError("No records with valid search text found")

    # Generate embeddings
    print("Generating embeddings...")
    search_texts = df_valid['search_text'].tolist()
    embeddings = generate_embeddings(search_texts)
    df_valid['vector'] = embeddings

    print(f"Generated {len(embeddings)} embeddings with dimension {len(embeddings[0])}")

    # Prepare data for LanceDB
    # Convert all columns to appropriate types for LanceDB
    lancedb_data = []
    for idx, row in df_valid.iterrows():
        record = {
            'identifier': str(row['identifier']) if pd.notna(row['identifier']) else '',
            'title': str(row['title']) if pd.notna(row['title']) else '',
            'description': str(row['description']) if pd.notna(row['description']) else '',
            'source': str(row['source']) if pd.notna(row['source']) else '',
            'date': str(row['date']) if pd.notna(row['date']) else '',
            'collection_time': str(row['collection_time']) if pd.notna(row['collection_time']) else '',
            'open_type': str(row['open_type']) if pd.notna(row['open_type']) else '',
            'license': str(row['license']) if pd.notna(row['license']) else '',
            'tags': str(row['tags']) if pd.notna(row['tags']) else '',
            'language': str(row['language']) if pd.notna(row['language']) else '',
            'format': str(row['format']) if pd.notna(row['format']) else '',
            'text': str(row['text']) if pd.notna(row['text']) else '',
            'word_count': int(row['word_count']) if pd.notna(row['word_count']) else 0,
            'token_count': int(row['token_count']) if pd.notna(row['token_count']) else 0,
            'data_file': str(row['data_file']) if pd.notna(row['data_file']) else '',
            # 'extra_metadata': str(row['extra_metadata']) if pd.notna(row['extra_metadata']) else '',
            # 'search_text': str(row['search_text']),
            'vector': row['vector'],
        }
        lancedb_data.append(record)

    if os.path.exists(LANCEDB_PATH):
        print(f"🗑️  Deleting old database folder: {LANCEDB_PATH}...")
        shutil.rmtree(LANCEDB_PATH)
    os.makedirs(LANCEDB_PATH, exist_ok=True)

    # Create LanceDB database and table
    print(f"Creating LanceDB at: {LANCEDB_PATH}")
    db = lancedb.connect(str(LANCEDB_PATH))

    # Drop existing table if it exists
    table_name = "ndl_core_datasets"
    if table_name in db.table_names():
        db.drop_table(table_name)
        print(f"Dropped existing table: {table_name}")

    # Create table with data
    table = db.create_table(table_name, lancedb_data, mode="overwrite")
    print(f"Created table '{table_name}' with {len(lancedb_data)} records")

    # Create vector search index for faster similarity search
    print("Creating vector search index...")
    table.create_index(
        metric="cosine",
        num_partitions=256,
        num_sub_vectors=96,
        replace=True
    )
    print("Vector search index created successfully")

    print(f"LanceDB index saved to: {LANCEDB_PATH}")

    return {
        "total_records": len(df),
        "indexed_records": len(lancedb_data),
        "embedding_dimension": len(embeddings[0]) if embeddings else 0,
        "lancedb_path": str(LANCEDB_PATH),
        "table_name": table_name,
    }

