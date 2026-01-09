import lancedb
from sentence_transformers import SentenceTransformer
from pathlib import Path
import os

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
LANCEDB_PATH = TARGET_DIR / "lancedb_search_index"

TABLE_NAME = "ndl_core_datasets"
MODEL_NAME = "all-MiniLM-L6-v2"
QUERY = "Police use of force"


def run_test():
    if not os.path.exists(LANCEDB_PATH):
        print(f"Error: Database folder '{LANCEDB_PATH}' not found.")
        return

    print(f"Loading model '{MODEL_NAME}'...")
    try:
        model = SentenceTransformer(MODEL_NAME)
    except Exception as e:
        print(f"Error loading model: {e}")
        return

    print(f"Connecting to LanceDB at '{LANCEDB_PATH}'...")
    try:
        db = lancedb.connect(LANCEDB_PATH)
        table = db.open_table(TABLE_NAME)
    except Exception as e:
        print(f"Error opening table: {e}")
        print("   Did you rename the folder correctly to 'lancedb_search_index'?")
        return

    # Perform Search
    print(f"Searching for: '{QUERY}'")
    query_vector = model.encode(QUERY)

    # Get list of columns to return (Everything EXCEPT 'vector')
    # This prevents the massive vector array from cluttering your screen
    columns_to_show = [col for col in table.schema.names if col != "vector"]

    results = (
        table.search(query_vector)
        .metric("cosine")
        .select(columns_to_show)  # <--- Filter out the vector column
        .limit(3)  # <--- Return top 3 results
        .to_pandas()
    )

    # 5. Display Results
    print("\nSearch Complete. Top Results:\n")

    # Loop through results and print them clearly
    for index, row in results.iterrows():
        print(f"--- Result {index + 1} (Distance: {row['_distance']:.4f}) ---")
        print(f"📂 Title:       {row.get('title', 'N/A')}")
        print(f"📄 Description: {row.get('description', 'N/A')[:200]}...")  # Truncate long descriptions
        print(f"🏷️  Tags:        {row.get('tags', 'N/A')}")
        print("-" * 50)


if __name__ == "__main__":
    run_test()