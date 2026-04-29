arr = [2, 3, 5, 4, 5, 3, 4]

result = 0
for num in arr:
    result ^= num  # XOR result = result ^ num

print(result)