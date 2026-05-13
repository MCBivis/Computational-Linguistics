from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
from transformers import AutoModelForCausalLM, AutoTokenizer

from fastembed import SparseTextEmbedding


DEFAULT_GPT_MODEL = "Qwen/Qwen2.5-7B-Instruct"
BM25_MODEL = "Qdrant/bm25"
_sparse_model = SparseTextEmbedding(model_name=BM25_MODEL)

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


def build_paragraphs(graph_paths: List[Path]) -> List[NodeParagraph]:
    paragraphs = []

    for graph_path in graph_paths:
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


def sparse_to_dict(sparse_embedding) -> Dict[int, float]:
    return dict(zip(sparse_embedding.indices.tolist(),
                    sparse_embedding.values.tolist()))


def sparse_score(
    query_dict: Dict[int, float],
    doc_dict: Dict[int, float],
) -> float:
    score = 0.0

    smaller, larger = (
        (query_dict, doc_dict)
        if len(query_dict) < len(doc_dict)
        else (doc_dict, query_dict)
    )

    for idx, value in smaller.items():
        if idx in larger:
            score += value * larger[idx]

    return score


def top_k_sparse(
    query_embedding,
    document_embeddings,
    k: int,
) -> List[int]:

    query_dict = sparse_to_dict(query_embedding)

    scores = []

    for idx, doc_embedding in enumerate(document_embeddings):
        doc_dict = sparse_to_dict(doc_embedding)

        sim = sparse_score(query_dict, doc_dict)

        scores.append((idx, sim))

    scores.sort(key=lambda x: x[1], reverse=True)

    return [idx for idx, _ in scores[:k]]


def build_prompt(question: str, contexts: List[str]) -> str:
    joined = "\n\n".join(contexts)
    return (
        "Дай ответ на данный вопрос, используя информацию только из текста:\n"
        f"\n{question}\n\n"
        f"Текст:\n{joined}\n\n"
    )


def _is_reasoner_model(model_name: str) -> bool:
    lower = model_name.lower()
    return "r1" in lower or "reasoner" in lower


def _build_chat_prompt(prompt: str) -> List[Dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Ты помощник для RAG. "
                "Отвечай строго на русском языке, только по фактам из контекста. "
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
    graph_paths: List[Path],
    n_first: int,
    m_second: int,
    llm_model: str,
    no_llm: bool,
) -> Dict[str, Any]:
    paragraphs = build_paragraphs(graph_paths)
    texts = [p.text for p in paragraphs]
    index_embeddings = list(_sparse_model.embed(texts))

    query_embedding = next(_sparse_model.embed([question]))

    n_ids = top_k_sparse(query_embedding, index_embeddings, n_first)
    n_contexts = [texts[i] for i in n_ids]

    first_prompt = build_prompt(question, n_contexts)
    first_answer = (
        ""
        if no_llm
        else generate_answer(first_prompt, model_name=llm_model)
    )

    answer_embedding = next(_sparse_model.embed([first_answer]))
    m_ids = top_k_sparse(answer_embedding, index_embeddings, m_second)
    merged_ids = list(dict.fromkeys(n_ids + m_ids))
    merged_contexts = [texts[i] for i in merged_ids]

    final_prompt = build_prompt(question, merged_contexts)
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
        "first_answer": first_answer,
        "final_answer": final_answer,
        "first_prompt": first_prompt,
        "final_prompt": final_prompt,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lab 28/04/2026: RAG over ontology graph.json")
    parser.add_argument(
        "--graphs",
        nargs="+",
        default=[
            "graph.json",
            "graph2.json",
            "ontology.json",
            "ontology_all_films.json",
        ],
        help="List of graph json files",
    )
    parser.add_argument("--question", required=True, help="User question")
    parser.add_argument("--n-first", type=int, default=5, help="Top-N nodes for first retrieval")
    parser.add_argument("--m-second", type=int, default=4, help="Top-M nodes for second retrieval from first answer")
    parser.add_argument("--llm-model", default=DEFAULT_GPT_MODEL)
    parser.add_argument("--no-llm", action="store_true", help="Skip text generation, test retrieval only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_rag(
        question=args.question,
        graph_paths=[Path(p) for p in args.graphs],
        n_first=args.n_first,
        m_second=args.m_second,
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

    print("\n=== Финальный ответ ===")
    print(result["final_answer"])

    print("\n=== Статистика ===")
    print(f"Количество Узлов(NodeParagraph): {len(result['paragraphs'])}")

if __name__ == "__main__":
    main()
