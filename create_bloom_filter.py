import hashlib
import bitarray
import base64
import pandas as pd

# 1. Load the AFINN word list and filter bad words with valence <= -4
afinn_url = 'https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt'
afinn_df = pd.read_csv(afinn_url, sep='\t', header=None, names=['word', 'valence'])
bad_words_df = afinn_df[afinn_df['valence'] <= -4]
bad_words = bad_words_df['word'].tolist()

# 2. Initialize the Bloom Filter
bloom_size = 1500  # Adjust size as needed
bloom_filter = bitarray.bitarray(bloom_size)
bloom_filter.setall(0)

# 3. Function to hash a word with multiple seeds
def hash_word(word, seed, bloom_size):
    hash_obj = hashlib.md5((word + str(seed)).encode())
    return int(hash_obj.hexdigest(), 16) % bloom_size

# 4. Populate the Bloom Filter with bad words
num_hashes = 3
for word in bad_words:
    print(f"Processing word: {word}")  # Print the word being processed
    for seed in range(num_hashes):
        index = hash_word(word, seed, bloom_size)
        bloom_filter[index] = 1
        print(f"  Seed: {seed}, Index: {index}")  # Print the hash index for each seed

# 5. Convert Bloom Filter to Base64 and save locally
bloom_base64 = base64.b64encode(bloom_filter.tobytes()).decode('utf-8')
local_path = '/home/taralthota/big-data-repo/bloom_filter_base64.txt'
with open(local_path, 'w') as f:
    f.write(bloom_base64)

print("Bloom filter creation completed and saved to bloom_filter_base64.txt")
