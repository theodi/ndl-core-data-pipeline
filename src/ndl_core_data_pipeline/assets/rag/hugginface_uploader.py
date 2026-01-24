import os
from huggingface_hub import HfApi
from pathlib import Path

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
LOCAL_FOLDER = "lancedb_search_index"
LANCEDB_PATH = TARGET_DIR / LOCAL_FOLDER

REPO_ID = "theodi/ndl-core-rag-index"

# 1. Verify the Manifest Exists (Prove it to yourself!)
manifest_path = f"{LANCEDB_PATH}/ndl_core_datasets.lance/_latest.manifest"

def upload():
    # if os.path.exists(manifest_path):
    #     size = os.path.getsize(manifest_path)
    #     print(f"Found hidden manifest at: {manifest_path}")
    #     print(f"Size: {size} bytes (Should be > 0)")
    # else:
    #     print(f"CRITICAL ERROR: Manifest is missing at {manifest_path}")
    #     print("   You must re-run the 'create table' script first.")
    #     exit()

    # 2. Upload with Force (Fixes the LFS Pointer issue)
    api = HfApi()

    api.upload_folder(
        folder_path=LANCEDB_PATH,
        path_in_repo=LOCAL_FOLDER,  # Uploads to root/lancedb_search_index/
        repo_id=REPO_ID,
        repo_type="dataset",
        commit_message="Fix LFS pointers and upload real LanceDB files"
    )

    print("\n🎉 Upload Complete!")
    print("👉 Now restart your Hugging Face Space (Factory Reboot).")

if __name__ == "__main__":
    upload()