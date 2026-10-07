import pandas as pd

file_path = r".\data\Industrial Asset-Level Electrical Energy Dataset from a Manufacturing Facility (15-Minute Aggregates)\data_csv\asset_id=ahu_a\dt_utc=2024-12-31\part-2024-12-31.csv"

df = pd.read_csv(file_path)

print(df.head())