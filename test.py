import os
import pandas as pd

df = pd.read_csv("2017.csv",low_memory=False)
cols = df.columns

# Create the 'csv_files' folder if it doesn't exist
folder_name = "./csv_files"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

for i in set(df['Issue Date']):
    j = i.split("/")
    year=int(j[2])
    if year>2017 or year<2016:
        continue
    filename = f"{j[2]}-{j[0]}-{j[1]}.csv"
    file_path = os.path.join(folder_name, filename)
    print(file_path)
    df.loc[df['Issue Date'] == i].to_csv(file_path, index=False, columns=cols)
