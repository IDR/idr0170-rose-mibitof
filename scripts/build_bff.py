import pandas as pd

# File paths
annotation_csv = "idr0170-experimentA-annotation.csv"
biofile_csv = "idr0170-biofile-finder.csv"
output_csv = "output.csv"

# Read CSVs
ann = pd.read_csv(annotation_csv)
bio = pd.read_csv(biofile_csv)

# Build a mapping from image name to file path (using endswith)
image_to_path = {}
for fp in bio['File Path']:
    image_name = fp.split('/')[-1]
    if image_name in image_to_path:
        raise ValueError(f"Duplicate image name found: {image_name}")
    image_to_path[image_name] = fp

# Map file paths to annotation rows
def find_file_path(image_name):
    return image_to_path[image_name]

ann['File Path'] = ann['Image Name'].apply(find_file_path)

# Write to output
ann.to_csv(output_csv, index=False)
