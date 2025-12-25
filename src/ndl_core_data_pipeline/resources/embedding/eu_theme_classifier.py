import torch
import pandas as pd
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm
from ndl_core_data_pipeline.resources.embedding.eu_data_themes import THEME_DEFINITIONS

# set based on test data analysis
SIMILARITY_THRESHOLD = 0.3
MAX_TAGS_PER_FILE = 3

model = SentenceTransformer('all-MiniLM-L6-v2')

theme_codes = list(THEME_DEFINITIONS.keys())
theme_descriptions = list(THEME_DEFINITIONS.values())

print("Encoding themes...")
theme_embeddings = model.encode(theme_descriptions, convert_to_tensor=True)


def get_multilabels_batch(text_list, threshold=0.35, top_k=3):
    """
    Returns a list of lists. Each item is a list of theme codes.
    e.g. [['AGRI', 'ENVI'], ['ECON'], ...]
    """
    doc_embeddings = model.encode(text_list, convert_to_tensor=True)

    # Each row contains the similarity scores for one document against all 13 themes
    cosine_scores = util.cos_sim(doc_embeddings, theme_embeddings)

    batch_results = []
    for scores in cosine_scores:
        # 'scores' is a tensor of 13 floats
        valid_indices = (scores > threshold).nonzero(as_tuple=True)[0]
        valid_scores = scores[valid_indices]
        sorted_indices_of_valid = torch.argsort(valid_scores, descending=True)

        # Take only the top K
        final_indices = valid_indices[sorted_indices_of_valid][:top_k]
        tags = [theme_codes[idx] for idx in final_indices.tolist()]

        batch_results.append(tags)

    return batch_results


def classify_eu_themes(data_to_tag):
    batch_size = 64
    all_tags = []

    records = data_to_tag.to_dict('records')
    for i in tqdm(range(0, len(records), batch_size)):
        batch = records[i:i + batch_size]
        batch_texts = [prepare_text(r) for r in batch]

        # Get tags
        batch_tags = get_multilabels_batch(
            batch_texts,
            threshold=SIMILARITY_THRESHOLD,
            top_k=MAX_TAGS_PER_FILE
        )
        all_tags.extend(batch_tags)

    data_to_tag['predicted_themes'] = all_tags
    return data_to_tag

def prepare_text(row):
    """Combines name and description for better context."""
    text = str(row.get('name', ''))
    if pd.notna(row.get('description')) and row.get('description'):
        text += f" {row['description']}"
    if pd.notna(row.get('text')) and row.get('text'):
        text += f" {row['text'][:1000]}"  # limit to first 1000 chars
    return text

if __name__ == "__main__":
    data = [
        {"name": "corn_prices.csv", "description": "Market rates for wheat and corn"},
        {"name": "green_energy_report.pdf", "description": "Impact of wind farms on local bird biodiversity"},
        {"name": "unknown.txt", "description": "random text about nothing"}
    ]
    tagged_df = classify_eu_themes(pd.DataFrame(data))

    print(tagged_df[['name', 'predicted_themes']])