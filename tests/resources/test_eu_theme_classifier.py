import importlib
import unittest
import pandas as pd

from ndl_core_data_pipeline.resources.embedding.eu_theme_classifier import classify_eu_themes

class TestEUThemeClassifier(unittest.TestCase):

    def test_classify_eu_themes_example(self):
        data = [
            {"name": "corn_prices.csv", "description": "Market rates for wheat and corn"},
            {"name": "green_energy_report.pdf", "description": "Impact of wind farms on local bird biodiversity"},
            {"name": "unknown.txt", "description": "random text about nothing"},
        ]
        df = pd.DataFrame(data)

        tagged_df = classify_eu_themes(df)
        print(tagged_df[['name', 'predicted_themes']])

        expected = [["AGRI"], ["ENVI"], []]
        result = tagged_df["predicted_themes"].tolist()
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()

