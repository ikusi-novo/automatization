import hashlib

def hash_string(input_string: str) -> str:
    return hashlib.sha256(input_string.encode()).hexdigest()

if __name__ == "__main__":
    print(hash_string("col4"))