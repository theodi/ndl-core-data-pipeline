from huggingface_hub import HfApi
from pathlib import Path

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
STRUCTURED_FOLDER = "structured"
STRUCTURED_PATH = TARGET_DIR / STRUCTURED_FOLDER

REPO_ID = "theodi/ndl-core-structured-data"


def delete_all_except_readme():
    """Delete all files from the HuggingFace dataset except README.md"""
    api = HfApi()

    # List all files in the repository
    repo_files = api.list_repo_files(repo_id=REPO_ID, repo_type="dataset")

    # Filter out README.md and .gitattributes (keep these)
    files_to_delete = [f for f in repo_files if f not in ["README.md", ".gitattributes"]]

    if not files_to_delete:
        print("No files to delete (only README.md found).")
        return

    print(f"Deleting {len(files_to_delete)} files from {REPO_ID}...")

    # Delete files in batches using delete_files
    api.delete_files(
        repo_id=REPO_ID,
        repo_type="dataset",
        delete_patterns=files_to_delete,
        commit_message="Delete all files except README.md before re-upload"
    )

    print(f"✅ Deleted {len(files_to_delete)} files.")


def upload_structured_data():
    """Upload all content from target/structured folder to HuggingFace"""
    api = HfApi()

    if not STRUCTURED_PATH.exists():
        print(f"❌ Error: Structured data folder not found at {STRUCTURED_PATH}")
        return

    print(f"Uploading structured data from {STRUCTURED_PATH} to {REPO_ID}...")

    api.upload_folder(
        folder_path=STRUCTURED_PATH,
        path_in_repo=STRUCTURED_FOLDER,
        repo_id=REPO_ID,
        repo_type="dataset",
        commit_message="Upload structured data files"
    )

    print("\n🎉 Upload Complete!")


def main():
    """Main function to delete existing content and upload new structured data"""
    print(f"Starting upload process for {REPO_ID}")
    print(f"Source folder: {STRUCTURED_PATH}")
    print("-" * 50)

    # Step 1: Delete all existing content except README.md
    delete_all_except_readme()

    # Step 2: Upload new structured data
    upload_structured_data()

    print("-" * 50)
    print("✅ Process complete!")


if __name__ == "__main__":
    main()

