import pandas as pd 
import random


# Generate a list of unique random numbers
n = 5  # Number of random numbers
li = random.sample(range(1, 101), n)


print(pd.DataFrame(li))
