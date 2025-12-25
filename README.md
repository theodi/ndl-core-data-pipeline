# National Data Library Core Data Pipeline

The data pipeline supporting the AI-ready core of the National Data Library

THis project uses [Dagster](https://dagster.io/) as the orchestration framework. Two main pipelines are defined:
- **ingest_pipeline**: Ingests raw data from various sources, performs initial processing, and stores it in a staging area.
- **process_pipeline**: Processes the staged data, applies transformations, and prepares it for final storage and deployment.


### Installing dependencies

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |


## Install system packages



# Additional system requirement for OCR

Install tesseract and poppler to your system. pdf2image requires the poppler utilities to convert PDF pages to images. On macOS you can install poppler with:

```bash
brew install tesseract
or
sudo apt install tesseract-ocr
```

```bash
brew install poppler
or
sudo apt install poppler-utils
```

### Running the data pipeline

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.
