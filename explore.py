import pandas as pd
import numpy as np
import glob

print(
pd.read_csv("./data/paradise_papers.edges.csv").rel_type.unique()
)
