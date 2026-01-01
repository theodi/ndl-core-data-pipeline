"""PII anonymization helpers using Microsoft Presidio.

Public API:
- anonymize_text_presidio(text: str, language: str = "en") -> str

"""
from tqdm import tqdm

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()

operators = {
    "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "xxx@xxx.xx"}),
    "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "xx-xxxx-xxxx"}),
}

def anonymize_text(text: str, language: str = "en") -> str:
    """Anonymize emails and UK phone numbers in `text` using Presidio.

    Raises ImportError if presidio packages are not available.
    """
    if not isinstance(text, str) or not text.strip():
        return text

    results = analyzer.analyze(text=text,
                               language=language,
                               entities=["EMAIL_ADDRESS", "PHONE_NUMBER"],
                               return_decision_process=False)

    anonymized_result = anonymizer.anonymize(
        text=text,
        analyzer_results=results,
        operators=operators
    )

    return anonymized_result.text


def run_batch_process(df, batch_size=100):
    """
    Manually iterates over the dataframe in chunks.
    This is often 10x faster than df.apply() for heavy NLP tasks.
    """
    # Filter: Get only the indices that need processing
    # (e.g., only rows where format is 'text' and text is not empty)
    mask = (df['format'] == 'text') & (df['text'].notna())
    target_indices = df[mask].index

    # Split indices into chunks
    # e.g., [0, 1, 2... 99], [100, 101... 199]
    chunks = [target_indices[i:i + batch_size] for i in range(0, len(target_indices), batch_size)]

    print(f"Processing {len(target_indices)} rows in {len(chunks)} batches...")

    for chunk_indices in tqdm(chunks, desc="Anonymizing Batches"):
        raw_texts = df.loc[chunk_indices, 'text'].tolist()

        processed_texts = [anonymize_text(t) for t in raw_texts]

        # C. Assign back to DataFrame (Vectorized assignment)
        df.loc[chunk_indices, 'text'] = processed_texts

    return df
