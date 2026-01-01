"""PII anonymization helpers using Microsoft Presidio.

Public API:
- anonymize_text_presidio(text: str, language: str = "en") -> str

"""

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig


def anonymize_text_presidio(text: str, language: str = "en") -> str:
    """Anonymize emails and UK phone numbers in `text` using Presidio.

    Raises ImportError if presidio packages are not available.
    """
    if not text:
        return text

    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()

    results = analyzer.analyze(text=text,
                               language=language,
                               entities=["EMAIL_ADDRESS", "PHONE_NUMBER"],
                               return_decision_process=False)

    operators = {
        "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "xxx@xxx.xx"}),
        "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "xx-xxxx-xxxx"}),
    }

    anonymized_result = anonymizer.anonymize(
        text=text,
        analyzer_results=results,
        operators=operators
    )

    return anonymized_result.text
