from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from lab_10_03_2026.embeddings import get_chunks, get_embeddings


def _read_texts_from_dir(path: Path) -> List[str]:
    texts: List[str] = []
    for p in sorted(path.glob("*.txt")):
        texts.append(p.read_text(encoding="utf-8", errors="ignore"))
    return texts


def _top_pairs(sim: np.ndarray, top_k: int) -> List[Tuple[int, int, float]]:
    if sim.ndim != 2 or sim.shape[0] != sim.shape[1]:
        raise ValueError("Expected a square similarity matrix.")
    n = sim.shape[0]
    sim = sim.copy()
    np.fill_diagonal(sim, -np.inf)

    pairs: List[Tuple[int, int, float]] = []
    for _ in range(min(top_k, n * (n - 1) // 2)):
        idx = int(np.argmax(sim))
        i, j = divmod(idx, n)
        score = float(sim[i, j])
        if not np.isfinite(score):
            break
        pairs.append((i, j, score))
        sim[i, j] = -np.inf
        sim[j, i] = -np.inf
    return pairs


def main() -> None:
    path = "D:\\Github\\Computational-Linguistics\\data\\texts"
    chunk_size = 50
    overlap = 15
    top = 10

    base = Path(path)
    if not base.exists() or not base.is_dir():
        raise SystemExit(f"Directory not found: {base}")

    texts = _read_texts_from_dir(base)
    if not texts:
        raise SystemExit(f"No .txt files found in: {base}")

    chunks = get_chunks(texts, chunk_size=chunk_size, overlap=overlap)
    if len(chunks) < 2:
        raise SystemExit("Not enough chunks to compare (need at least 2).")

    emb = get_embeddings(chunks)
    sim = cosine_similarity(emb, emb)

    print(f"Loaded texts: {len(texts)}")
    print(f"Chunks: {len(chunks)}")
    print("")
    for i, j, score in _top_pairs(sim, top):
        print(f"score={score:.4f}  chunk[{i}] vs chunk[{j}]")
        print(f"A: {chunks[i][:240].replace('\\n', ' ')}{'...' if len(chunks[i]) > 240 else ''}")
        print(f"B: {chunks[j][:240].replace('\\n', ' ')}{'...' if len(chunks[j]) > 240 else ''}")
        print("")


if __name__ == "__main__":
    main()

