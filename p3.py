
import pandas as pd
import numpy as np
from pandas_profiling import ProfileReport

df=pd.read_csv('/home/ubuntu/data/ruby.csv', low_memory = True)
df=df.iloc[:3,:3]

profile = ProfileReport(df, minimal=True)
profile.to_file("output.html")

