
from pathlib import Path
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import re

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
NUM_REDUCERS = 2


def mapper(file_path):
    text = file_path.read_text(encoding="utf-8").lower()
    words = re.findall(r"\b[\w']+\b", text)
    return file_path.name, [(word, 1) for word in words]


def partition(word):
    return sum(ord(char) for char in word) % NUM_REDUCERS


def run_mapreduce():
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Folder not found: {INPUT_DIR}")

    files = sorted(INPUT_DIR.iterdir())
    files = [file for file in files if file.is_file()]

    if not files:
        raise ValueError("No input files found.")

    # Map
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(mapper, files))

    # Combine
    combined = []

    for filename, pairs in results:
        counts = Counter(word for word, count in pairs)
        print(f"\n{filename}: {pairs}")
        print(f"Combined: {dict(counts)}")
        combined.extend(counts.items())

    # Shuffle and partition
    reducers = [defaultdict(int) for _ in range(NUM_REDUCERS)]

    for word, count in combined:
        reducer_id = partition(word)
        reducers[reducer_id][word] += count

    # Reduce and write output
    OUTPUT_DIR.mkdir(exist_ok=True)

    for old_file in OUTPUT_DIR.glob("part-*.txt"):
        old_file.unlink()

    for reducer_id, word_counts in enumerate(reducers):
        output_file = OUTPUT_DIR / f"part-{reducer_id}.txt"

        with output_file.open("w", encoding="utf-8") as file:
            for word, count in sorted(word_counts.items()):
                file.write(f"{word}\t{count}\n")

        print(f"\nReducer {reducer_id}: {dict(word_counts)}")

    print("\nMapReduce completed successfully.")

    for output_file in sorted(OUTPUT_DIR.glob("part-*.txt")):
        print(f"\n{output_file}:")
        print(output_file.read_text(encoding="utf-8"))


if __name__ == "__main__":
    run_mapreduce()
