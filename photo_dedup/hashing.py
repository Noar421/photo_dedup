import xxhash

BUFFER = 256 * 1024

def file_hash(path):
    h = xxhash.xxh3_128()
    with open(path, "rb") as f:
        while chunk := f.read(BUFFER):
            h.update(chunk)
    return h.hexdigest()
