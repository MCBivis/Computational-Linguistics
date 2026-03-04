from __future__ import annotations

from typing import List, Optional, Sequence, Tuple, Union, overload

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from sentence_transformers import SentenceTransformer


DEFAULT_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

_MODEL_CACHE: dict[Tuple[str, Optional[str]], "SentenceTransformer"] = {}

def _get_model(model_name: str, device: Optional[str]) -> "SentenceTransformer":
    key = (model_name, device)
    if key not in _MODEL_CACHE:
        _MODEL_CACHE[key] = SentenceTransformer(model_name, device=device)
    return _MODEL_CACHE[key]


def get_chunks(
    texts: Union[str, Sequence[str]],
    *,
    chunk_size: int = 200,
    overlap: int = 50,
    min_chunk_size: int = 20,
) -> List[str]:

    if isinstance(texts, str):
        texts_list = [texts]
    else:
        texts_list = list(texts)

    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")

    step = chunk_size - overlap
    if step <= 0:
        step = chunk_size

    out: List[str] = []
    for text in texts_list:
        if not text or not text.strip():
            continue

        words = text.split()
        if not words:
            continue

        chunks_for_text: List[str] = []
        i = 0
        while i < len(words):
            chunk_words = words[i : i + chunk_size]
            if not chunk_words:
                break
            chunks_for_text.append(" ".join(chunk_words).strip())
            i += step

        if not chunks_for_text:
            continue

        filtered: List[str] = []
        for c in chunks_for_text:
            if len(c.split()) >= min_chunk_size:
                filtered.append(c)

        out.extend(filtered if filtered else chunks_for_text[:1])

    return out

def get_embeddings(
    texts: Union[str, Sequence[str]],
    *,
    model_name: str = DEFAULT_MODEL_NAME,
    device: Optional[str] = None,
    normalize: bool = True,
    batch_size: int = 32,
) -> np.ndarray:
    single = isinstance(texts, str)
    texts_list = [texts] if single else list(texts)

    model = _get_model(model_name, device)
    emb = model.encode(
        texts_list,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=normalize,
        show_progress_bar=False,
    )
    emb = np.asarray(emb, dtype=np.float32)
    return emb[0] if single else emb


def cos_compare(
    emb_a: Union[Sequence[float], np.ndarray],
    emb_b: Union[Sequence[float], np.ndarray],
) -> Union[float, np.ndarray]:

    a = np.asarray(emb_a, dtype=np.float32)
    b = np.asarray(emb_b, dtype=np.float32)

    a2 = a.reshape(1, -1) if a.ndim == 1 else a
    b2 = b.reshape(1, -1) if b.ndim == 1 else b

    sim = cosine_similarity(a2, b2)

    if a.ndim == 1 and b.ndim == 1:
        return float(sim[0, 0])
    if a.ndim == 1 and b.ndim == 2:
        return sim[0]
    if a.ndim == 2 and b.ndim == 1:
        return sim[:, 0]
    return sim

