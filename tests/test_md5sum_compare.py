import pytest
import os
import tempfile
from md5sum_compare.main import (
    md5sum_async,
    process_file,
    generate_manifest,
    load_manifest,
    compare_manifests,
    compare
)
import pandas as pd

# Utility function to create test files with specific content
def create_test_file(dir_path, filename, content):
    file_path = os.path.join(dir_path, filename)
    with open(file_path, 'w') as f:
        f.write(content)
    return file_path

@pytest.mark.asyncio
async def test_md5sum_async():
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = create_test_file(tmpdirname, "test.txt", "Hello, World!")
        checksum = await md5sum_async(file_path)
        assert checksum == "65a8e27d8879283831b664bd8b7f0ad4"

@pytest.mark.asyncio
async def test_process_file():
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = create_test_file(tmpdirname, "test.txt", "Hello, World!")
        path, checksum = await process_file(file_path)
        assert path == file_path
        assert checksum == "65a8e27d8879283831b664bd8b7f0ad4"

@pytest.mark.asyncio
async def test_generate_manifest():
    with tempfile.TemporaryDirectory() as tmpdirname:
        file1_path = create_test_file(tmpdirname, "test1.txt", "Hello, World!")
        file2_path = create_test_file(tmpdirname, "test2.txt", "Another file content")
        
        output_file = os.path.join(tmpdirname, "manifest.txt")
        await generate_manifest(tmpdirname, output_file)
        
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert "test1.txt 65a8e27d8879283831b664bd8b7f0ad4\n" in lines
            assert "test2.txt f41f69f6f6eb0d631ea0d9a45e2ed04d\n" in lines

def test_load_manifest():
    manifest_content = """test1.txt 65a8e27d8879283831b664bd8b7f0ad4
test2.txt a9c91d9759d65b8d3b23ed7efc2b4bbd
"""
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        tmpfile.write(manifest_content.encode())
        tmpfile.close()
        manifest = load_manifest(tmpfile.name)
        assert manifest["test1.txt"] == "65a8e27d8879283831b664bd8b7f0ad4"
        assert manifest["test2.txt"] == "a9c91d9759d65b8d3b23ed7efc2b4bbd"
        os.remove(tmpfile.name)

def test_compare_manifests():
    source_manifest = {
        "test1.txt": "65a8e27d8879283831b664bd8b7f0ad4",
        "test2.txt": "a9c91d9759d65b8d3b23ed7efc2b4bbd"
    }
    destination_manifest = {
        "test1.txt": "65a8e27d8879283831b664bd8b7f0ad4",
        "test3.txt": "d41d8cd98f00b204e9800998ecf8427e"
    }
    missing_files, extra_files, mismatched_files = compare_manifests(source_manifest, destination_manifest)
    
    assert missing_files == {"test2.txt"}
    assert extra_files == {"test3.txt"}
    assert mismatched_files == set()

def test_compare():
    source_manifest_content = """test1.txt 65a8e27d8879283831b664bd8b7f0ad4
test2.txt a9c91d9759d65b8d3b23ed7efc2b4bbd
"""
    destination_manifest_content = """test1.txt 65a8e27d8879283831b664bd8b7f0ad4
test3.txt d41d8cd98f00b204e9800998ecf8427e
"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        source_manifest_path = os.path.join(tmpdirname, "source_manifest.txt")
        destination_manifest_path = os.path.join(tmpdirname, "destination_manifest.txt")
        output_csv_path = os.path.join(tmpdirname, "comparison_results.csv")
        
        with open(source_manifest_path, 'w') as f:
            f.write(source_manifest_content)
        
        with open(destination_manifest_path, 'w') as f:
            f.write(destination_manifest_content)
        
        df = compare(source_manifest_path, destination_manifest_path, output_csv_path)
        
        assert "missing" in df.columns
        assert "extra" in df.columns
        assert "hash_mismatch" in df.columns
        
        assert len(df[df['missing'].notnull()]) == 1
        assert df['missing'].iloc[0] == "test2.txt"
        
        assert len(df[df['extra'].notnull()]) == 1
        assert df['extra'].iloc[0] == "test3.txt"
        
        assert df[df['hash_mismatch'].notnull()].empty

        assert os.path.exists(output_csv_path)

if __name__ == "__main__":
    pytest.main()