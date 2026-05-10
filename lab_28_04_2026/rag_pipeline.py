from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
from transformers import AutoModelForCausalLM, AutoTokenizer

from lab_10_03_2026.embeddings import cos_compare, get_embeddings

import re

DEFAULT_GPT_MODEL = "Qwen/Qwen2.5-7B-Instruct"
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
MAX_FRAGMENT_CHARS = 700

TECH_KEYS = {
    "uri",
    "id",
    "labels",
    "params",
    "ontology_uri",
    "is_toggled",
    "toggled_data",
    "file",
    "connected_file",
    "text_mentions",
    "graph_direction",
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2000/01/rdf-schema#comment",

}

RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL_URI = "http://www.w3.org/2000/01/rdf-schema#label"

_LLM_CACHE: Dict[str, Tuple[Any, Any]] = {}


@dataclass
class NodeParagraph:
    node_id: str
    title: str
    text: str

@dataclass
class TextFragment:
    node_uri: str
    entity_name: str
    fragment: str

@dataclass
class SentenceInfo:
    start: int
    end: int
    text: str

@dataclass
class SentenceLookup:
    sentences: List[SentenceInfo]
    word_to_sentence: List[int]

def load_markup(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_sentence_lookup(words: List[str]) -> SentenceLookup:

    sentences: List[SentenceInfo] = []

    sentence_endings = {".", "!", "?"}

    current_words: List[str] = []
    start = 0

    for idx, word in enumerate(words):

        if not word:
            continue

        current_words.append(word)

        if word in sentence_endings:

            sentence_text = " ".join(current_words).strip()

            sentences.append(
                SentenceInfo(
                    start=start,
                    end=idx,
                    text=sentence_text,
                )
            )

            current_words = []
            start = idx + 1

    if current_words:
        sentences.append(
            SentenceInfo(
                start=start,
                end=len(words) - 1,
                text=" ".join(current_words).strip(),
            )
        )

    word_to_sentence = [-1] * len(words)

    for sentence_idx, sentence in enumerate(sentences):

        for word_idx in range(sentence.start, sentence.end + 1):
            word_to_sentence[word_idx] = sentence_idx

    return SentenceLookup(
        sentences=sentences,
        word_to_sentence=word_to_sentence,
    )


def build_word_array(text_with_ids: Dict[str, str]) -> List[str]:
    max_id = max(int(k) for k in text_with_ids.keys())

    words = [""] * (max_id + 1)

    for k, v in text_with_ids.items():
        cleaned = str(v).replace("\n", "").strip()
        words[int(k)] = cleaned

    return words


def extract_sentence_fragment(
    lookup: SentenceLookup,
    pos_start: int,
    neighbour_sentences: int = 1,
) -> str:

    if pos_start >= len(lookup.word_to_sentence):
        return ""

    target_index = lookup.word_to_sentence[pos_start]

    if target_index < 0:
        return ""

    left = max(0, target_index - neighbour_sentences)

    right = min(
        len(lookup.sentences),
        target_index + neighbour_sentences + 1,
    )

    fragment = " ".join(
        sentence.text
        for sentence in lookup.sentences[left:right]
    )

    fragment = re.sub(r"\s+", " ", fragment).strip()

    return fragment[:MAX_FRAGMENT_CHARS]


def collect_text_fragments(
    markup_data: Dict[str, Any],
    target_node_uris: set[str],
) -> List[TextFragment]:

    words = build_word_array(markup_data["textWithIds"])
    lookup = build_sentence_lookup(words)

    fragments = []

    for entity in markup_data["entites"]:

        node_uri = entity["node_uri"]

        if node_uri not in target_node_uris:
            continue

        fragment = extract_sentence_fragment(
            lookup,
            entity["pos_start"],
        )

        node = entity.get("node", {})
        title = _node_label(node)

        fragments.append(
            TextFragment(
                node_uri=node_uri,
                entity_name=title,
                fragment=fragment,
            )
        )

    return fragments


def retrieve_text_fragments(
    markup_paths: List[Path],
    target_uris: set[str],
    query_embedding: np.ndarray,
    embedding_model: str,
    top_k: int = 3,
) -> List[TextFragment]:

    all_fragments: List[TextFragment] = []

    for markup_path in markup_paths:

        markup_data = load_markup(markup_path)

        fragments = collect_text_fragments(
            markup_data,
            target_uris,
        )

        all_fragments.extend(fragments)

    if not all_fragments:
        return []

    unique_fragments: Dict[str, TextFragment] = {}

    for fragment in all_fragments:

        cleaned = fragment.fragment.strip()

        if not cleaned:
            continue

        unique_fragments[cleaned] = fragment

    all_fragments = list(unique_fragments.values())

    fragment_texts = [
        f.fragment
        for f in all_fragments
    ]

    fragment_embeddings = get_embeddings(
        fragment_texts,
        model_name=embedding_model,
    )

    fragment_ids = top_k_indices(
        query_embedding,
        fragment_embeddings,
        k=top_k,
    )

    return [
        all_fragments[i]
        for i in fragment_ids
    ]


def _tail(uri_or_name: str) -> str:
    if "#" in uri_or_name:
        return uri_or_name.rsplit("#", 1)[1]
    return uri_or_name.rstrip("/").rsplit("/", 1)[-1]


def _first_lang_value(value: Any, preferred_lang: str = "@ru") -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and preferred_lang in item:
                return item.replace(preferred_lang, "").strip()
        for item in value:
            if isinstance(item, str):
                return item.split("@", 1)[0].strip()
    return None


def _humanize_key(key: str) -> str:
    tail = _tail(key)
    if not tail:
        return key
    out = []
    prev_upper = False
    for ch in tail:
        is_upper = "A" <= ch <= "Z"
        if is_upper and out and not prev_upper:
            out.append(" ")
        out.append(ch)
        prev_upper = is_upper
    return "".join(out).replace("_", " ").strip()


def _is_property_node(node: Dict[str, Any]) -> bool:
    labels = node.get("data", {}).get("labels", [])
    labels_str = " ".join(labels).lower() if isinstance(labels, list) else str(labels).lower()
    return (
        "datatypeproperty" in labels_str
        or "objecttypeproperty" in labels_str
        or "owl#datatypeproperty" in labels_str
        or "owl#objectproperty" in labels_str
    )


def _is_class_node(node: Dict[str, Any]) -> bool:
    labels = node.get("data", {}).get("labels", [])
    labels_str = " ".join(labels).lower() if isinstance(labels, list) else str(labels).lower()
    return "owl#class" in labels_str


def _node_label(node: Dict[str, Any]) -> str:
    data = node.get("data", {})
    raw_label = data.get(RDFS_LABEL_URI)
    label = _first_lang_value(raw_label, preferred_lang="@ru") or _first_lang_value(raw_label, preferred_lang="@en")
    if label:
        return label
    uri = data.get("uri") or node.get("id")
    return _tail(str(uri))


def _iter_param_lines(
    node: Dict[str, Any],
    property_name_by_uri: Dict[str, str],
) -> Iterable[str]:
    data = node.get("data", {})
    params_values = data.get("params_values", {})
    if not isinstance(params_values, dict):
        return
    for key, value in params_values.items():
        if key in TECH_KEYS:
            continue
        key_name = property_name_by_uri.get(key) or _humanize_key(key)
        if isinstance(value, list):
            val = ", ".join(str(x).split("@", 1)[0] for x in value)
        else:
            val = str(value).split("@", 1)[0]
        if val and val.strip():
            yield f"{key_name}: {val.strip()}"


def _build_graph_maps(graph: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    nodes = graph.get("nodes", [])
    arcs = graph.get("arcs", [])
    node_by_id = {node.get("id"): node for node in nodes if node.get("id")}
    return node_by_id, arcs


def _build_property_dictionary(node_by_id: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    property_name_by_uri: Dict[str, str] = {}
    for node in node_by_id.values():
        if not _is_property_node(node):
            continue
        data = node.get("data", {})
        uri = data.get("uri") or node.get("id")
        if not uri:
            continue
        property_name_by_uri[str(uri)] = _node_label(node)
    return property_name_by_uri


def _relation_name(uri: str, property_name_by_uri: Dict[str, str]) -> str:
    return property_name_by_uri.get(uri) or _humanize_key(uri)


def build_paragraphs(graph_path: Path) -> List[NodeParagraph]:
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    node_by_id, arcs = _build_graph_maps(graph)
    property_name_by_uri = _build_property_dictionary(node_by_id)

    inbound: Dict[str, List[Dict[str, Any]]] = {}
    outbound: Dict[str, List[Dict[str, Any]]] = {}
    for arc in arcs:
        src = arc.get("source")
        dst = arc.get("target")
        if src:
            outbound.setdefault(src, []).append(arc)
        if dst:
            inbound.setdefault(dst, []).append(arc)

    paragraphs: List[NodeParagraph] = []

    for node_id, node in node_by_id.items():
        if _is_property_node(node) or _is_class_node(node):
            continue

        title = _node_label(node)
        lines = [f"Название: {title}"]
        lines.extend(_iter_param_lines(node, property_name_by_uri))

        for arc in outbound.get(node_id, []):
            arc_uri = str(arc.get("data", {}).get("uri", ""))
            target_node = node_by_id.get(arc.get("target"))
            if not target_node:
                continue
            relation = _relation_name(arc_uri, property_name_by_uri)
            target_name = _node_label(target_node)
            if arc_uri == RDF_TYPE_URI:
                lines.append(f"Имеет тип: {target_name}")
            else:
                lines.append(f"{relation}: {target_name}")

        for arc in inbound.get(node_id, []):
            arc_uri = str(arc.get("data", {}).get("uri", ""))
            source_node = node_by_id.get(arc.get("source"))
            if not source_node:
                continue
            relation = _relation_name(arc_uri, property_name_by_uri)
            source_name = _node_label(source_node)
            if arc_uri == RDF_TYPE_URI:
                lines.append(f"Тип для: {source_name}")
            else:
                lines.append(f"{relation}: {source_name}")

        text = "\n".join(dict.fromkeys(lines))
        paragraphs.append(NodeParagraph(node_id=node_id, title=title, text=text))

    return paragraphs


def top_k_indices(query_embedding: np.ndarray, index_embeddings: np.ndarray, k: int) -> List[int]:
    sims = cos_compare(query_embedding, index_embeddings)
    scores = np.asarray(sims, dtype=np.float32)
    if scores.ndim != 1:
        scores = scores.reshape(-1)
    k = min(k, len(scores))
    if k <= 0:
        return []
    top_ids = np.argsort(scores)[::-1][:k]
    return [int(i) for i in top_ids]


def build_prompt(
    question: str,
    ontology_contexts: List[str],
    text_fragments: List[TextFragment],
) -> str:

    ontology_text = "\n\n".join(ontology_contexts)

    fragments_text = "\n\n".join(
        f"[{fragment.entity_name}] {fragment.fragment}"
        for fragment in text_fragments
    )

    return f"""
Ответь на заданный вопрос: {question}

Используя основной текст:
{ontology_text}

Дополняя свой ответ данными текстами:
{fragments_text}
"""


def _is_reasoner_model(model_name: str) -> bool:
    lower = model_name.lower()
    return "r1" in lower or "reasoner" in lower


def _build_chat_prompt(prompt: str) -> List[Dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Отвечай строго на русском языке. "
                "Никаких рассуждений о процессе, только итоговый ответ. "
                "Формат: 1-2 коротких предложения."
            ),
        },
        {"role": "user", "content": prompt},
    ]


def _get_llm(model_name: str) -> Tuple[Any, Any]:
    if model_name in _LLM_CACHE:
        return _LLM_CACHE[model_name]

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(model_name, device_map="auto")
    _LLM_CACHE[model_name] = (tokenizer, model)
    return tokenizer, model


def _clean_model_answer(text: str) -> str:
    cleaned = text.strip()
    if "</think>" in cleaned:
        cleaned = cleaned.rsplit("</think>", 1)[-1].strip()

    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    uniq_lines: List[str] = []
    for line in lines:
        if not uniq_lines or uniq_lines[-1] != line:
            uniq_lines.append(line)
    cleaned = "\n".join(uniq_lines).strip()
    return cleaned

def generate_answer(
    prompt: str,
    model_name: str = DEFAULT_GPT_MODEL,
    max_new_tokens: int = 160,
) -> str:
    tokenizer, model = _get_llm(model_name)

    if tokenizer.pad_token_id is None and tokenizer.eos_token_id is not None:
        tokenizer.pad_token = tokenizer.eos_token

    prompt_text: str
    has_chat_template = bool(getattr(tokenizer, "chat_template", None))
    if _is_reasoner_model(model_name) or has_chat_template:
        prompt_text = tokenizer.apply_chat_template(
            _build_chat_prompt(prompt),
            tokenize=False,
            add_generation_prompt=True,
        )
    else:
        prompt_text = prompt

    encoded = tokenizer(prompt_text, return_tensors="pt")
    encoded = {k: v.to(model.device) for k, v in encoded.items()}
    prompt_tokens = int(encoded["input_ids"].shape[1])

    generated_ids = model.generate(
        **encoded,
        max_new_tokens=max_new_tokens,
        do_sample=False,
        repetition_penalty=1.1,
        no_repeat_ngram_size=3,
        pad_token_id=tokenizer.pad_token_id,
        eos_token_id=tokenizer.eos_token_id,
    )
    answer_ids = generated_ids[0][prompt_tokens:]
    raw_text = tokenizer.decode(answer_ids, skip_special_tokens=True).strip()

    return _clean_model_answer(raw_text)


def run_rag(
    question: str,
    graph_path: Path,
    markup_paths: List[Path],
    n_first: int,
    m_second: int,
    embedding_model: str,
    llm_model: str,
    no_llm: bool,
) -> Dict[str, Any]:
    paragraphs = build_paragraphs(graph_path)
    texts = [p.text for p in paragraphs]
    index_embeddings = get_embeddings(texts, model_name=embedding_model)

    query_embedding = get_embeddings(question, model_name=embedding_model)
    n_ids = top_k_indices(query_embedding, index_embeddings, n_first)
    n_contexts = [texts[i] for i in n_ids]

    first_prompt = build_prompt(question, n_contexts, [])
    first_answer = (
        ""
        if no_llm
        else generate_answer(first_prompt, model_name=llm_model)
    )

    answer_embedding = get_embeddings(first_answer, model_name=embedding_model)
    m_ids = top_k_indices(answer_embedding, index_embeddings, m_second)
    merged_ids = list(dict.fromkeys(n_ids + m_ids))
    merged_contexts = [texts[i] for i in merged_ids]

    target_uris = {
        paragraphs[i].node_id
        for i in merged_ids
    }

    best_fragments = retrieve_text_fragments(
        markup_paths=markup_paths,
        target_uris=target_uris,
        query_embedding=query_embedding,
        embedding_model=embedding_model,
        top_k=3,
    )

    final_prompt = build_prompt(
        question,
        merged_contexts,
        best_fragments,
    )
    final_answer = (
        "[LLM выключена: ответ не генерировался]"
        if no_llm
        else generate_answer(final_prompt, model_name=llm_model)
    )

    return {
        "paragraphs": paragraphs,
        "n_ids": n_ids,
        "m_ids": m_ids,
        "merged_ids": merged_ids,
        "best_fragments": best_fragments,
        "first_answer": first_answer,
        "final_answer": final_answer,
        "first_prompt": first_prompt,
        "final_prompt": final_prompt,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lab 28/04/2026: RAG over ontology graph.json")
    parser.add_argument("--graph", default="graph.json", help="Path to ontology graph.json")
    parser.add_argument("--markups", nargs="+", default=["belosnezhka.json", "krasavitsa.json", "repunzel.json", "zolushka.json"], help="Markup json files")
    parser.add_argument("--question", required=True, help="User question")
    parser.add_argument("--n-first", type=int, default=5, help="Top-N nodes for first retrieval")
    parser.add_argument("--m-second", type=int, default=4, help="Top-M nodes for second retrieval from first answer")
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--llm-model", default=DEFAULT_GPT_MODEL)
    parser.add_argument("--no-llm", action="store_true", help="Skip text generation, test retrieval only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_rag(
        question=args.question,
        graph_path=Path(args.graph),
        markup_paths=[Path(p) for p in args.markups],
        n_first=args.n_first,
        m_second=args.m_second,
        embedding_model=args.embedding_model,
        llm_model=args.llm_model,
        no_llm=args.no_llm,
    )

    print("=== Первая выборка N ===")
    for idx in result["n_ids"]:
        para = result["paragraphs"][idx]
        print(f"\n[{idx}] {para.title}\n{para.text}\n")

    print("=== Предварительный ответ ===")
    print(result["first_answer"])

    print("\n=== Вторая выборка M ===")
    for idx in result["m_ids"]:
        para = result["paragraphs"][idx]
        print(f"\n[{idx}] {para.title}\n{para.text}\n")

    print("=== Объединенная выборка N + M ===")
    for idx in result["merged_ids"]:
        para = result["paragraphs"][idx]
        print(f"- [{idx}] {para.title}")

    print("\n=== Использованные текстовые фрагменты ===")

    for idx, fragment in enumerate(result["best_fragments"], start=1):
        print(
            f"\n[{idx}] {fragment.entity_name}\n"
            f"{fragment.fragment}\n"
        )

    print("\n=== Финальный ответ ===")
    print(result["final_answer"])


if __name__ == "__main__":
    main()
