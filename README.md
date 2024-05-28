# md5sum_compare

Utility for computing md5sum values for all files in a directory, descending into subdirectories, and writing the results to a manifest file. The manifest file can then be compared with another, for example to verify that two copies of a file tree are exactly equivalent.


## Example Usage

1. **Generate the manifest for the source directory:**
   ```bash
   python script.py generate /path/to/source_directory source_manifest.txt
   ```

2. **Copy the manifest file to the destination server and generate the manifest for the destination directory:**
   ```bash
   python script.py generate /path/to/destination_directory destination_manifest.txt
   ```

3. **Compare the manifests and optionally output to a CSV file:**
   ```bash
   python script.py compare source_manifest.txt destination_manifest.txt --output_csv comparison_results.csv
   ```