from itertools import product

# Possible values for min_1, max_1, min_2, max_2 (multiples of 5 less than 100)
values = list(range(2, 15, 3))

valid_combinations = []

# Generate all possible tuples (min_1, max_1, min_2, max_2)
for min_1, max_1, min_2, max_2 in product(values, repeat=4):
    if (
        min_1 < max_1 and
        min_2 < max_2 and
        max_1 + max_2 < 40
    ):
        valid_combinations.append((min_1, max_1, min_2, max_2))
        print(f"{(min_1, max_1, min_2, max_2)},")

# Number of valid combinations
print("-----------")
print(len(valid_combinations))
