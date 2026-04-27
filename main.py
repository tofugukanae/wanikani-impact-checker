from os import getenv
from pathlib import Path
import csv
import html
import json
import time
from io import StringIO

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from starlette.middleware.sessions import SessionMiddleware
from sudachipy import dictionary, tokenizer as sudachi_tokenizer

load_dotenv()

DATABASE_URL = "sqlite:///./app.db"
EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(exist_ok=True)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Subject(Base):
    __tablename__ = "subjects"

    id = Column(Integer, primary_key=True, index=True)
    subject_id = Column(Integer, unique=True, index=True, nullable=False)
    object = Column(String, nullable=False)
    subject_type = Column(String, nullable=False)
    characters = Column(String, nullable=True)
    level = Column(Integer, nullable=False)
    data_json = Column(Text, nullable=False)


Base.metadata.create_all(bind=engine)

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=getenv("SECRET_KEY", "dev-secret-key"),
)

templates = Jinja2Templates(directory="templates")

APP_PASSWORD = getenv("APP_PASSWORD", "")
WANIKANI_API_TOKEN = getenv("WANIKANI_API_TOKEN", "")
WANIKANI_API_REVISION = getenv("WANIKANI_API_REVISION", "20170710")
WANIKANI_SUBJECTS_URL = "https://api.wanikani.com/v2/subjects"
latest_movement_candidates = []
latest_movement_level_map = {}

REQUIRED_HEADERS = ["characters", "subject_type", "current_level", "new_level"]
ALLOWED_SUBJECT_TYPES = {"vocabulary", "kanji", "radical"}
SUPPORTED_SUBJECT_TYPES = {"vocabulary", "kanji"}

HEADER_ALIASES = {
    "characters": ["characters", "Subjects", "subjects"],
    "subject_type": ["subject_type", "Type", "type"],
    "current_level": ["current_level", "今-Level", "current level"],
    "new_level": ["new_level", "新-Level", "new level"],
}

tokenizer_obj = dictionary.Dictionary().create()
tokenizer_mode = sudachi_tokenizer.Tokenizer.SplitMode.C


def pick_header(actual_headers, aliases):
    for alias in aliases:
        if alias in actual_headers:
            return alias
    return None


def normalize_subject_type(subject_type_raw: str) -> str:
    subject_type_map = {
        "vocab": "vocabulary",
        "vocabulary": "vocabulary",
        "kanji": "kanji",
        "radical": "radical",
    }
    return subject_type_map.get(subject_type_raw.lower(), subject_type_raw.lower())


def validate_csv_text(text: str):
    reader = csv.DictReader(StringIO(text))
    headers = reader.fieldnames or []

    header_map = {}
    missing_headers = []

    for canonical_name, aliases in HEADER_ALIASES.items():
        matched = pick_header(headers, aliases)
        if matched:
            header_map[canonical_name] = matched
        else:
            missing_headers.append(canonical_name)

    rows = []
    blocking_error_count = 0
    seen_keys = set()
    db = SessionLocal()

    if missing_headers:
        return {
            "headers": headers,
            "missing_headers": missing_headers,
            "rows": [],
            "blocking_error_count": 1,
        }

    try:
        for index, row in enumerate(reader, start=2):
            status = "ok"
            message = ""

            characters = (row.get(header_map["characters"]) or "").strip()
            subject_type_raw = (row.get(header_map["subject_type"]) or "").strip()
            current_level_raw = (row.get(header_map["current_level"]) or "").strip()
            new_level_raw = (row.get(header_map["new_level"]) or "").strip()

            subject_type = normalize_subject_type(subject_type_raw)

            row_type = "movement" if current_level_raw else "addition"
            current_level = None
            new_level = None
            resolved_subject_id = None
            db_level = None

            if not characters or not subject_type or not new_level_raw:
                status = "missing_field"
                message = "One or more required fields are empty."
            elif subject_type not in ALLOWED_SUBJECT_TYPES:
                status = "invalid_subject_type"
                message = "subject_type must be vocabulary, kanji, or radical."
            else:
                try:
                    new_level = int(new_level_raw)

                    if row_type == "movement":
                        current_level = int(current_level_raw)

                    if not (1 <= new_level <= 60) or (
                        row_type == "movement" and not (1 <= current_level <= 60)
                    ):
                        status = "invalid_level"
                        message = "Levels must be between 1 and 60."
                except ValueError:
                    status = "invalid_level"
                    message = "Levels must be integers."

            row_key = (characters, subject_type)

            if status == "ok":
                if row_key in seen_keys:
                    status = "duplicate_row"
                    message = "This subject appears more than once in the CSV."
                else:
                    seen_keys.add(row_key)

            if status == "ok" and subject_type not in SUPPORTED_SUBJECT_TYPES:
                status = "unsupported_type"
                message = "This subject type is not supported in v1 analysis."

            if status == "ok" and row_type == "movement":
                subject = (
                    db.query(Subject)
                    .filter(Subject.characters == characters, Subject.subject_type == subject_type)
                    .first()
                )

                if not subject:
                    status = "not_found"
                    message = "No matching subject was found in the local WaniKani data."
                else:
                    resolved_subject_id = subject.subject_id
                    db_level = subject.level

                    if current_level != db_level:
                        status = "current_level_mismatch"
                        message = f"CSV current_level is {current_level}, but WaniKani data says {db_level}."

            if status == "ok" and row_type == "addition":
                subject = (
                    db.query(Subject)
                    .filter(Subject.characters == characters, Subject.subject_type == subject_type)
                    .first()
                )

                if subject:
                    resolved_subject_id = subject.subject_id
                    db_level = subject.level

            if status == "ok" and row_type == "movement":
                if current_level == new_level:
                    status = "unchanged_level"
                    message = "current_level and new_level are the same."
                elif new_level < current_level:
                    status = "moved_down_no_risk"
                    message = "This item moved down, so it is not a v1 analysis target."

            if status in {
                "missing_field",
                "invalid_subject_type",
                "invalid_level",
                "duplicate_row",
                "not_found",
                "current_level_mismatch",
            }:
                blocking_error_count += 1

            rows.append(
                {
                    "line_number": index,
                    "row_type": row_type,
                    "characters": characters,
                    "subject_type": subject_type,
                    "current_level": current_level_raw,
                    "new_level": new_level_raw,
                    "resolved_subject_id": resolved_subject_id,
                    "db_level": db_level,
                    "status": status,
                    "message": message,
                }
            )

        return {
            "headers": headers,
            "missing_headers": [],
            "rows": rows,
            "blocking_error_count": blocking_error_count,
        }
    finally:
        db.close()


def get_context_candidates(validated_rows):
    candidates = []

    for row in validated_rows:
        if (
            row["status"] == "ok"
            and row.get("row_type", "movement") == "movement"
            and row["subject_type"] == "vocabulary"
            and row["resolved_subject_id"]
        ):
            candidates.append(row)
        elif (
            row["status"] == "ok"
            and row.get("row_type") == "addition"
            and row["subject_type"] in {"vocabulary", "kanji"}
        ):
            candidates.append(row)

    return candidates


def get_collocation_candidates(validated_rows):
    candidates = []

    for row in validated_rows:
        if (
            row["status"] == "ok"
            and row.get("row_type", "movement") == "movement"
            and row["subject_type"] == "vocabulary"
            and row["resolved_subject_id"]
        ):
            candidates.append(row)
        elif (
            row["status"] == "ok"
            and row.get("row_type") == "addition"
            and row["subject_type"] in {"vocabulary", "kanji"}
        ):
            candidates.append(row)

    return candidates


def get_support_candidates(validated_rows):
    candidates = []

    for row in validated_rows:
        if (
            row["status"] == "ok"
            and row.get("row_type", "movement") == "movement"
            and row["subject_type"] in {"vocabulary", "kanji"}
            and row["resolved_subject_id"]
        ):
            candidates.append(row)
        elif (
            row["status"] == "ok"
            and row.get("row_type") == "addition"
            and row["subject_type"] in {"vocabulary", "kanji"}
        ):
            candidates.append(row)

    return candidates


def build_movement_level_map(candidates):
    movement_levels = {}

    for candidate in candidates:
        subject_id = candidate.get("resolved_subject_id")
        new_level_raw = candidate.get("new_level")

        if not subject_id or not new_level_raw:
            continue

        try:
            movement_levels[int(subject_id)] = int(new_level_raw)
        except (TypeError, ValueError):
            continue

    return movement_levels


def get_result_type_with_final_levels(
    old_level,
    changed_final_level,
    used_in_subject_id,
    used_in_current_level,
    movement_level_map,
):
    used_in_final_level = movement_level_map.get(
        int(used_in_subject_id),
        int(used_in_current_level),
    )

    if changed_final_level <= used_in_final_level:
        return None, used_in_final_level

    if old_level <= used_in_final_level:
        return "newly_broken", used_in_final_level

    if old_level > used_in_final_level:
        return "already_broken", used_in_final_level

    return None, used_in_final_level


def get_candidate_result_type(candidate, used_in_subject_id, used_in_current_level, movement_level_map):
    changed_final_level = int(candidate["new_level"])
    used_in_final_level = movement_level_map.get(
        int(used_in_subject_id),
        int(used_in_current_level),
    )

    if changed_final_level <= used_in_final_level:
        return None, used_in_final_level

    if candidate.get("row_type") == "addition":
        return "new_item_used_below_level", used_in_final_level

    old_level = int(candidate["current_level"])

    if old_level <= used_in_final_level:
        return "newly_broken", used_in_final_level

    if old_level > used_in_final_level:
        return "already_broken", used_in_final_level

    return None, used_in_final_level


def get_candidate_display_levels(candidate):
    if candidate.get("row_type") == "addition":
        return "", int(candidate["new_level"])

    return int(candidate["current_level"]), int(candidate["new_level"])


def sort_candidates_by_match_priority(candidates):
    priority = {
        "vocabulary": 0,
        "kanji": 1,
    }

    return sorted(candidates, key=lambda candidate: priority.get(candidate.get("subject_type"), 2))


def should_skip_kanji_fallback(candidate, match_key, vocabulary_match_keys):
    return candidate.get("subject_type") == "kanji" and match_key in vocabulary_match_keys


def remember_vocabulary_match(candidate, match_key, vocabulary_match_keys):
    if candidate.get("subject_type") == "vocabulary":
        vocabulary_match_keys.add(match_key)


def extract_context_sentences(subject: Subject):
    try:
        payload = json.loads(subject.data_json)
    except Exception:
        return []

    data = payload.get("data", {})
    context_sentences = data.get("context_sentences", []) or []

    cleaned = []
    for index, sentence in enumerate(context_sentences):
        ja_text = (sentence.get("ja") or "").strip()
        en_text = (sentence.get("en") or "").strip()

        if ja_text:
            sentence_label = f"#{index + 1}"
            is_wildcard = index >= 2

            cleaned.append(
                {
                    "ja": ja_text,
                    "en": en_text,
                    "position": index,
                    "sentence_label": sentence_label,
                    "is_wildcard": is_wildcard,
                }
            )

    return cleaned


def extract_parts_of_speech(subject: Subject):
    try:
        payload = json.loads(subject.data_json)
    except Exception:
        return []

    data = payload.get("data", {})
    return data.get("parts_of_speech", []) or []


SUPPORT_CONTENT_FIELDS = [
    ("meaning_mnemonic", "Meaning Mnemonic"),
    ("reading_mnemonic", "Reading Mnemonic"),
    ("meaning_hint", "Meaning Hint"),
    ("reading_hint", "Reading Hint"),
]


def extract_support_content(subject: Subject):
    try:
        payload = json.loads(subject.data_json)
    except Exception:
        return []

    data = payload.get("data", {})
    content = []

    for field_name, label in SUPPORT_CONTENT_FIELDS:
        text = (data.get(field_name) or "").strip()

        if text:
            content.append(
                {
                    "field_name": field_name,
                    "label": label,
                    "text": text,
                }
            )

    return content


def get_support_confidence(subject_type):
    if subject_type == "vocabulary":
        return "high"

    if subject_type == "kanji":
        return "medium"

    return "medium"


def is_kanji_character(character):
    codepoint = ord(character)
    return (
        0x3400 <= codepoint <= 0x4DBF
        or 0x4E00 <= codepoint <= 0x9FFF
        or 0xF900 <= codepoint <= 0xFAFF
    )


def find_all_spans(text, needle):
    spans = []
    start = 0

    while needle:
        index = text.find(needle, start)

        if index == -1:
            break

        spans.append((index, index + len(needle)))
        start = index + 1

    return spans


def spans_overlap(first, second):
    return first[0] < second[1] and second[0] < first[1]


def detect_content_vocabulary(text, vocabulary_subjects):
    detected = {}
    occupied_spans = []

    sorted_subjects = sorted(
        [subject for subject in vocabulary_subjects if subject.characters],
        key=lambda subject: len(subject.characters or ""),
        reverse=True,
    )

    for subject in sorted_subjects:
        for span in find_all_spans(text, subject.characters):
            if any(spans_overlap(span, occupied) for occupied in occupied_spans):
                continue

            detected[subject.subject_id] = subject
            occupied_spans.append(span)
            break

    token_forms = set()

    for token in tokenize_japanese(text):
        surface = token.surface()
        dictionary_form = token.dictionary_form()

        if surface:
            token_forms.add(surface)

        if dictionary_form:
            token_forms.add(dictionary_form)

    for subject in vocabulary_subjects:
        if subject.subject_id in detected or not subject.characters:
            continue

        if subject.characters in token_forms:
            detected[subject.subject_id] = subject

    return list(detected.values()), occupied_spans


def detect_content_kanji(text, kanji_subjects, vocabulary_spans):
    kanji_by_character = {
        subject.characters: subject
        for subject in kanji_subjects
        if subject.characters
    }
    detected = {}

    for index, character in enumerate(text):
        if not is_kanji_character(character):
            continue

        character_span = (index, index + 1)

        if any(spans_overlap(character_span, vocabulary_span) for vocabulary_span in vocabulary_spans):
            continue

        subject = kanji_by_character.get(character)

        if subject:
            detected[subject.subject_id] = subject

    return list(detected.values())


def format_detected_items(input_text, vocabulary_subjects, kanji_subjects):
    vocab_items = sorted(vocabulary_subjects, key=lambda subject: (subject.level, subject.characters or ""))
    kanji_items = sorted(kanji_subjects, key=lambda subject: (subject.level, subject.characters or ""))

    if not vocab_items and not kanji_items:
        detected_items_text = "No items detected"

        return {
            "input_text": input_text,
            "input_text_html": html.escape(input_text).replace("\n", "<br>"),
            "detected_items": detected_items_text,
            "detected_items_html": html.escape(detected_items_text).replace("\n", "<br>"),
            "vocabulary_items": vocab_items,
            "kanji_items": kanji_items,
        }

    lines = ["Vocab"]

    if vocab_items:
        for subject in vocab_items:
            lines.append(f"{subject.characters} ({subject.level})")
    else:
        lines.append("None")

    if kanji_items:
        lines.extend(["", "Kanji"])

        for subject in kanji_items:
            lines.append(f"{subject.characters} ({subject.level})")

    detected_items_text = "\n".join(lines)

    return {
        "input_text": input_text,
        "input_text_html": html.escape(input_text).replace("\n", "<br>"),
        "detected_items": detected_items_text,
        "detected_items_html": html.escape(detected_items_text).replace("\n", "<br>"),
        "vocabulary_items": vocab_items,
        "kanji_items": kanji_items,
    }


def analyze_new_content_text(text):
    stripped_text = text.strip()

    if not stripped_text:
        return None

    db = SessionLocal()

    try:
        vocabulary_subjects = db.query(Subject).filter(Subject.subject_type == "vocabulary").all()
        kanji_subjects = db.query(Subject).filter(Subject.subject_type == "kanji").all()
        detected_vocabulary, vocabulary_spans = detect_content_vocabulary(
            stripped_text,
            vocabulary_subjects,
        )
        detected_kanji = detect_content_kanji(
            stripped_text,
            kanji_subjects,
            vocabulary_spans,
        )

        return format_detected_items(
            stripped_text,
            detected_vocabulary,
            detected_kanji,
        )
    finally:
        db.close()


def is_verb_like(parts_of_speech):
    normalized = [p.lower() for p in parts_of_speech]

    verb_keywords = [
        "verb",
        "suru verb",
        "transitive verb",
        "intransitive verb",
        "godan verb",
        "ichidan verb",
    ]

    for pos in normalized:
        for keyword in verb_keywords:
            if keyword in pos:
                return True

    return False


def get_confidence_and_note(match_method, changed_characters, wk_parts_of_speech):
    if match_method == "sudachi_dictionary_form":
        if is_verb_like(wk_parts_of_speech):
            return "high", "Dictionary form match on verb-like item."
        return "medium", "Dictionary form match; verify semantic fit."

    if match_method == "sudachi_surface":
        if is_verb_like(wk_parts_of_speech):
            return "high", "Surface token match on verb-like item."
        return "medium", "Surface token match; verify semantic fit."

    if match_method in {"exact_substring_longest", "exact_substring"}:
        if len(changed_characters or "") <= 1:
            return "low", "Very short substring match; manual review recommended."
        return "medium", "Substring match; review context."

    return "medium", "Review recommended."


def sentence_has_longer_competing_match(sentence_ja: str, changed_characters: str, vocabulary_subjects):
    if not changed_characters:
        return False

    for subject in vocabulary_subjects:
        candidate = subject.characters or ""

        if not candidate:
            continue

        if candidate == changed_characters:
            continue

        if len(candidate) <= len(changed_characters):
            continue

        if changed_characters not in candidate:
            continue

        if candidate in sentence_ja:
            return True

    return False


def tokenize_japanese(text: str):
    tokens = tokenizer_obj.tokenize(text, tokenizer_mode)
    return tokens


def sentence_matches_candidate_with_sudachi(sentence_ja: str, changed_characters: str):
    if not changed_characters:
        return None

    tokens = tokenize_japanese(sentence_ja)

    for token in tokens:
        surface = token.surface()
        dictionary_form = token.dictionary_form()

        if surface == changed_characters:
            return "sudachi_surface"

        if dictionary_form == changed_characters:
            return "sudachi_dictionary_form"

    return None


def text_matches_candidate(text: str, changed_characters: str, subject_type: str, vocabulary_subjects):
    if not changed_characters:
        return None

    if subject_type == "kanji":
        if changed_characters in text:
            return "exact_substring"
        return None

    if changed_characters in text:
        if sentence_has_longer_competing_match(
            text,
            changed_characters,
            vocabulary_subjects,
        ):
            return None
        return "exact_substring_longest"

    return sentence_matches_candidate_with_sudachi(text, changed_characters)


def highlight_sentence_ja(sentence_ja: str, changed_characters: str, match_method: str):
    if not sentence_ja or not changed_characters:
        return sentence_ja

    if match_method in {"exact_substring_longest", "exact_substring"}:
        return sentence_ja.replace(
            changed_characters,
            f'<span style="color: red; font-weight: bold;">{changed_characters}</span>',
            1,
        )

    tokens = tokenize_japanese(sentence_ja)
    parts = []
    i = 0

    while i < len(tokens):
        token = tokens[i]
        surface = token.surface()
        dictionary_form = token.dictionary_form()

        should_highlight = False

        if match_method == "sudachi_surface" and surface == changed_characters:
            should_highlight = True
        elif match_method == "sudachi_dictionary_form" and dictionary_form == changed_characters:
            should_highlight = True

        if should_highlight:
            combined = surface
            j = i + 1

            while j < len(tokens):
                next_surface = tokens[j].surface()
                next_pos = tokens[j].part_of_speech()

                # 活用の続きっぽい token をまとめる
                if next_pos and (
                    next_pos[0] == "助動詞"
                    or next_pos[0] == "助詞"
                    or next_pos[0] == "動詞"
                    or next_pos[0] == "形容詞"
                    or next_pos[0] == "接尾辞"
                ):
                    combined += next_surface
                    j += 1
                    continue

                # ひらがなだけの短い後続もまとめる
                if next_surface and all("ぁ" <= ch <= "ん" for ch in next_surface):
                    combined += next_surface
                    j += 1
                    continue

                break

            parts.append(f'<span style="color: red; font-weight: bold;">{combined}</span>')
            i = j
        else:
            parts.append(surface)
            i += 1

    return "".join(parts)

def build_notion_highlighted_sentence(sentence_ja: str, changed_characters: str, match_method: str):
    if not sentence_ja or not changed_characters:
        return sentence_ja

    if match_method in {"exact_substring_longest", "exact_substring"}:
        return sentence_ja.replace(
            changed_characters,
            f"【{changed_characters}】",
            1,
        )

    tokens = tokenize_japanese(sentence_ja)
    parts = []
    i = 0

    while i < len(tokens):
        token = tokens[i]
        surface = token.surface()
        dictionary_form = token.dictionary_form()

        should_highlight = False

        if match_method == "sudachi_surface" and surface == changed_characters:
            should_highlight = True
        elif match_method == "sudachi_dictionary_form" and dictionary_form == changed_characters:
            should_highlight = True

        if should_highlight:
            combined = surface
            j = i + 1

            while j < len(tokens):
                next_surface = tokens[j].surface()
                next_pos = tokens[j].part_of_speech()

                if next_pos and (
                    next_pos[0] == "助動詞"
                    or next_pos[0] == "助詞"
                    or next_pos[0] == "動詞"
                    or next_pos[0] == "形容詞"
                    or next_pos[0] == "接尾辞"
                ):
                    combined += next_surface
                    j += 1
                    continue

                if next_surface and all("ぁ" <= ch <= "ん" for ch in next_surface):
                    combined += next_surface
                    j += 1
                    continue

                break

            parts.append(f"【{combined}】")
            i = j
        else:
            parts.append(surface)
            i += 1

    return "".join(parts)

def run_basic_analysis(candidates, movement_level_map):
    db = SessionLocal()
    results = []
    vocabulary_match_keys = set()

    try:
        vocabulary_subjects = db.query(Subject).filter(Subject.subject_type == "vocabulary").all()

        for candidate in sort_candidates_by_match_priority(candidates):
            changed_subject_id = candidate.get("resolved_subject_id")
            changed_characters = candidate["characters"]
            old_level, new_level = get_candidate_display_levels(candidate)
            row_type = candidate.get("row_type", "movement")

            changed_subject = None
            if changed_subject_id:
                changed_subject = (
                    db.query(Subject)
                    .filter(Subject.subject_id == changed_subject_id)
                    .first()
                )
            wk_parts_of_speech = extract_parts_of_speech(changed_subject) if changed_subject else []

            for subject in vocabulary_subjects:
                if changed_subject_id and subject.subject_id == changed_subject_id:
                    continue

                used_in_level = subject.level
                context_sentences = extract_context_sentences(subject)

                for sentence in context_sentences:
                    sentence_ja_raw = sentence.get("ja") or ""
                    sentence_en_raw = sentence.get("en") or ""

                    if not changed_characters:
                        continue

                    match_method = text_matches_candidate(
                        sentence_ja_raw,
                        changed_characters,
                        candidate["subject_type"],
                        vocabulary_subjects,
                    )

                    if not match_method:
                        continue

                    result_type, used_in_final_level = get_candidate_result_type(
                        candidate,
                        subject.subject_id,
                        used_in_level,
                        movement_level_map,
                    )

                    if not result_type:
                        continue

                    match_key = (
                        "context",
                        subject.subject_id,
                        sentence.get("sentence_label", ""),
                        changed_characters,
                    )

                    if should_skip_kanji_fallback(candidate, match_key, vocabulary_match_keys):
                        continue

                    remember_vocabulary_match(candidate, match_key, vocabulary_match_keys)

                    confidence, review_note = get_confidence_and_note(
                        match_method,
                        changed_characters,
                        wk_parts_of_speech,
                    )

                    if sentence.get("is_wildcard", False):
                        review_note = f"{review_note} Wildcard sentence; graded reader restriction does not apply."

                    sentence_ja_highlighted = highlight_sentence_ja(
                        sentence_ja_raw,
                        changed_characters,
                        match_method,
                    )

                    sentence_ja_for_notion = build_notion_highlighted_sentence(
                        sentence_ja_raw,
                        changed_characters,
                        match_method,
                    )

                    results.append(
                        {
                            "changed_subject_id": changed_subject_id,
                            "changed_characters": changed_characters,
                            "changed_item_display": (
                                f"{changed_characters} (new → {new_level})"
                                if row_type == "addition"
                                else f"{changed_characters} ({old_level} → {new_level})"
                            ),
                            "old_level": old_level,
                            "new_level": new_level,
                            "used_in_subject_id": subject.subject_id,
                            "used_in_characters": subject.characters,
                            "used_in_level": used_in_level,
                            "used_in_final_level": used_in_final_level,
                            "sentence_label": sentence.get("sentence_label", ""),
                            "sentence_ja": sentence_ja_raw,
                            "sentence_ja_highlighted": sentence_ja_highlighted or sentence_ja_raw,
                            "sentence_ja_for_notion": sentence_ja_for_notion or sentence_ja_raw,
                            "sentence_en": sentence_en_raw,
                            "match_method": match_method,
                            "result_type": result_type,
                            "wk_parts_of_speech": ", ".join(wk_parts_of_speech),
                            "confidence": confidence,
                            "review_note": review_note,
                            "used_in_display": f"{subject.characters} ({used_in_level}) {sentence.get('sentence_label', '')}",
                        }
                    )

        return results
    finally:
        db.close()


def run_support_content_analysis(candidates, movement_level_map):
    db = SessionLocal()
    results = []
    vocabulary_match_keys = set()
    review_note = (
        "Changed item is referenced in this support content and may now be "
        "above the used-in item level."
    )

    try:
        subjects = db.query(Subject).filter(Subject.subject_type.in_(["vocabulary", "kanji"])).all()

        for candidate in sort_candidates_by_match_priority(candidates):
            changed_subject_id = candidate.get("resolved_subject_id")
            changed_characters = candidate["characters"]
            old_level, new_level = get_candidate_display_levels(candidate)
            changed_subject_type = candidate["subject_type"]
            row_type = candidate.get("row_type", "movement")

            if not changed_characters:
                continue

            for subject in subjects:
                if changed_subject_id and subject.subject_id == changed_subject_id:
                    continue

                used_in_level = subject.level
                result_type, used_in_final_level = get_candidate_result_type(
                    candidate,
                    subject.subject_id,
                    used_in_level,
                    movement_level_map,
                )

                if not result_type:
                    continue

                for content in extract_support_content(subject):
                    if changed_characters not in content["text"]:
                        continue

                    match_key = (
                        "support",
                        subject.subject_id,
                        content["field_name"],
                        changed_characters,
                    )

                    if should_skip_kanji_fallback(candidate, match_key, vocabulary_match_keys):
                        continue

                    remember_vocabulary_match(candidate, match_key, vocabulary_match_keys)

                    results.append(
                        {
                            "changed_subject_id": changed_subject_id,
                            "changed_characters": changed_characters,
                            "changed_item_display": (
                                f"{changed_characters} (new → {new_level})"
                                if row_type == "addition"
                                else f"{changed_characters} ({old_level} → {new_level})"
                            ),
                            "old_level": old_level,
                            "new_level": new_level,
                            "used_in_subject_id": subject.subject_id,
                            "used_in_characters": subject.characters,
                            "used_in_level": used_in_level,
                            "used_in_final_level": used_in_final_level,
                            "used_in_display": f"{subject.characters} ({used_in_level})",
                            "content_type": content["label"],
                            "matched_text": content["text"],
                            "result_type": result_type,
                            "confidence": get_support_confidence(changed_subject_type),
                            "review_note": review_note,
                        }
                    )

        return results
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": None,
        },
    )

def build_analysis_results_csv(results):
    output = StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "changed_item",
        "old_level",
        "new_level",
        "used_in_item",
        "used_in_level",
        "result_type",
        "sentence_label",
        "sentence_jp",
        "sentence_en",
        "match_method",
        "wk_parts_of_speech",
        "confidence",
        "review_note",
    ])

    for result in results:
        writer.writerow([
        result.get("changed_characters", ""),
        result.get("old_level", ""),
        result.get("new_level", ""),
        result.get("used_in_characters", ""),
        result.get("used_in_level", ""),
        result.get("result_type", ""),
        result.get("sentence_label", ""),
        result.get("sentence_ja", ""),
        result.get("sentence_en", ""),
        result.get("match_method", ""),
        result.get("wk_parts_of_speech", ""),
        result.get("confidence", ""),
        result.get("review_note", ""),
    ])

    return output.getvalue()

def build_analysis_results_tsv(results):
    output = StringIO()
    writer = csv.writer(output, delimiter="\t", lineterminator="\n")

    writer.writerow([
        "changed_item",
        "old_level",
        "new_level",
        "used_in_item",
        "used_in_level",
        "result_type",
        "sentence_label",
        "sentence_jp",
        "sentence_en",
        "match_method",
        "wk_parts_of_speech",
        "confidence",
        "review_note",
    ])

    for result in results:
        writer.writerow([
            result.get("changed_characters", ""),
            result.get("old_level", ""),
            result.get("new_level", ""),
            result.get("used_in_characters", ""),
            result.get("used_in_level", ""),
            result.get("result_type", ""),
            result.get("sentence_label", ""),
            result.get("sentence_ja_for_notion", result.get("sentence_ja", "")),
            result.get("sentence_en", ""),
            result.get("match_method", ""),
            result.get("wk_parts_of_speech", ""),
            result.get("confidence", ""),
            result.get("review_note", ""),
        ])

    return output.getvalue()


def pick_row_value(row, *fieldnames):
    for fieldname in fieldnames:
        value = row.get(fieldname)
        if value is not None and str(value).strip():
            return str(value).strip()

    return ""


def resolve_collocation_subject(db, subject_id_raw, used_in_item, used_in_level_raw):
    subject_id = None

    if subject_id_raw:
        try:
            subject_id = int(subject_id_raw)
        except ValueError:
            return None, None, "invalid_subject_id", "subject_id must be an integer."

        subject = db.query(Subject).filter(Subject.subject_id == subject_id).first()

        if not subject:
            return subject_id, None, "not_found", "No matching subject was found in the local WaniKani data."

        return subject_id, subject, "ok", ""

    if not used_in_item:
        return None, None, "missing_field", "Either subject_id or used_in_item is required."

    query = db.query(Subject).filter(
        Subject.characters == used_in_item,
        Subject.subject_type == "vocabulary",
    )

    if used_in_level_raw:
        try:
            used_in_level = int(used_in_level_raw)
        except ValueError:
            return None, None, "invalid_level", "used_in_level must be an integer."

        query = query.filter(Subject.level == used_in_level)

    subject = query.first()

    if not subject:
        return None, None, "not_found", "No matching used_in_item was found in the local WaniKani data."

    return subject.subject_id, subject, "ok", ""


def resolve_changed_subject(db, changed_item):
    if not changed_item:
        return None

    return (
        db.query(Subject)
        .filter(Subject.characters == changed_item, Subject.subject_type == "vocabulary")
        .first()
    )


def parse_level(raw_value):
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def validate_collocations_csv_text(text: str):
    reader = csv.DictReader(StringIO(text))
    headers = reader.fieldnames or []
    has_subject_ref = "subject_id" in headers
    has_japanese = "japanese" in headers
    has_english = "english" in headers
    missing_headers = []

    if not has_subject_ref:
        missing_headers.append("subject_id")

    if not has_japanese:
        missing_headers.append("japanese")

    if not has_english:
        missing_headers.append("english")

    rows = []
    blocking_error_count = 0
    db = SessionLocal()

    if missing_headers:
        return {
            "headers": headers,
            "missing_headers": missing_headers,
            "rows": [],
            "blocking_error_count": 1,
        }

    try:
        for index, row in enumerate(reader, start=2):
            status = "ok"
            message = ""
            subject_id_raw = pick_row_value(row, "subject_id")
            japanese = pick_row_value(row, "japanese")
            english = pick_row_value(row, "english")
            pattern_of_use = pick_row_value(row, "pattern_of_use")
            collocation_id = pick_row_value(row, "id")
            subject_id = None
            subject_level = None
            subject_characters = ""

            if not subject_id_raw or not japanese:
                status = "missing_field"
                message = "subject_id and japanese are required."
            else:
                subject_id, subject, status, message = resolve_collocation_subject(
                    db,
                    subject_id_raw,
                    "",
                    "",
                )

            if status == "ok":
                subject_level = subject.level
                subject_characters = subject.characters or ""

            if status in {"missing_field", "invalid_subject_id", "invalid_level", "not_found"}:
                blocking_error_count += 1

            rows.append(
                {
                    "line_number": index,
                    "collocation_id": collocation_id,
                    "subject_id": subject_id,
                    "subject_id_raw": subject_id_raw,
                    "used_in_item": subject_characters,
                    "used_in_level_raw": subject_level,
                    "changed_item": "",
                    "changed_subject_id": None,
                    "old_level": "",
                    "new_level": "",
                    "subject_characters": subject_characters,
                    "subject_level": subject_level,
                    "pattern_of_use": pattern_of_use,
                    "japanese": japanese,
                    "english": english,
                    "status": status,
                    "message": message,
                }
            )

        return {
            "headers": headers,
            "missing_headers": [],
            "rows": rows,
            "blocking_error_count": blocking_error_count,
        }
    finally:
        db.close()


def collocation_matches_subject(
    collocation_ja: str,
    subject: Subject,
    vocabulary_subjects,
    token_surfaces,
    token_dictionary_forms,
):
    characters = subject.characters or ""

    if not characters:
        return None

    if characters in collocation_ja:
        if sentence_has_longer_competing_match(
            collocation_ja,
            characters,
            vocabulary_subjects,
        ):
            return None
        return "exact_substring_longest"

    if characters in token_surfaces:
        return "sudachi_surface"

    if characters in token_dictionary_forms:
        return "sudachi_dictionary_form"

    return None


def run_collocation_analysis(validated_rows, movement_candidates, movement_level_map):
    db = SessionLocal()
    results = []
    vocabulary_match_keys = set()

    try:
        vocabulary_subjects = db.query(Subject).filter(Subject.subject_type == "vocabulary").all()
        prioritized_candidates = sort_candidates_by_match_priority(movement_candidates)

        for row in validated_rows:
            if row["status"] != "ok" or row["subject_level"] is None:
                continue

            used_in_subject_id = row["subject_id"]
            used_in_level = int(row["subject_level"])
            collocation_ja = row["japanese"]
            tokens = tokenize_japanese(collocation_ja)
            token_surfaces = {token.surface() for token in tokens}
            token_dictionary_forms = {token.dictionary_form() for token in tokens}

            for candidate in prioritized_candidates:
                old_level, new_level = get_candidate_display_levels(candidate)
                row_type = candidate.get("row_type", "movement")
                candidate_subject = None
                changed_subject_id = candidate.get("resolved_subject_id")

                if changed_subject_id:
                    candidate_subject = (
                        db.query(Subject)
                        .filter(Subject.subject_id == changed_subject_id)
                        .first()
                    )

                if changed_subject_id and changed_subject_id == used_in_subject_id:
                    continue

                result_type, used_in_final_level = get_candidate_result_type(
                    candidate,
                    used_in_subject_id,
                    used_in_level,
                    movement_level_map,
                )

                if not result_type:
                    continue

                if candidate_subject and candidate["subject_type"] == "vocabulary":
                    match_method = collocation_matches_subject(
                        collocation_ja,
                        candidate_subject,
                        vocabulary_subjects,
                        token_surfaces,
                        token_dictionary_forms,
                    )
                else:
                    match_method = text_matches_candidate(
                        collocation_ja,
                        candidate["characters"],
                        candidate["subject_type"],
                        vocabulary_subjects,
                    )

                if not match_method:
                    continue

                changed_characters = (
                    candidate_subject.characters
                    if candidate_subject and candidate_subject.characters
                    else candidate["characters"]
                )
                match_key = (
                    "collocation",
                    row["collocation_id"] or row["line_number"],
                    used_in_subject_id,
                    changed_characters,
                )

                if should_skip_kanji_fallback(candidate, match_key, vocabulary_match_keys):
                    continue

                remember_vocabulary_match(candidate, match_key, vocabulary_match_keys)

                wk_parts_of_speech = extract_parts_of_speech(candidate_subject) if candidate_subject else []
                confidence, review_note = get_confidence_and_note(
                    match_method,
                    changed_characters,
                    wk_parts_of_speech,
                )

                highlighted_ja = highlight_sentence_ja(
                    collocation_ja,
                    changed_characters,
                    match_method,
                )
                notion_ja = build_notion_highlighted_sentence(
                    collocation_ja,
                    changed_characters,
                    match_method,
                )

                results.append(
                    {
                        "changed_subject_id": changed_subject_id,
                        "changed_characters": changed_characters,
                        "changed_item_display": (
                            f"{changed_characters} (new → {new_level})"
                            if row_type == "addition"
                            else f"{changed_characters} ({old_level} → {new_level})"
                        ),
                        "old_level": old_level,
                        "new_level": new_level,
                        "used_in_subject_id": used_in_subject_id,
                        "used_in_characters": row["subject_characters"],
                        "used_in_level": used_in_level,
                        "used_in_final_level": used_in_final_level,
                        "sentence_label": row["pattern_of_use"],
                        "sentence_ja": collocation_ja,
                        "sentence_ja_highlighted": highlighted_ja or collocation_ja,
                        "sentence_ja_for_notion": notion_ja or collocation_ja,
                        "sentence_en": row["english"],
                        "match_method": match_method,
                        "result_type": result_type,
                        "wk_parts_of_speech": ", ".join(wk_parts_of_speech),
                        "confidence": confidence,
                        "review_note": review_note,
                        "used_in_display": f"{row['subject_characters']} ({used_in_level}) {row['pattern_of_use']}",
                        "collocation_id": row["collocation_id"],
                        "pattern_of_use": row["pattern_of_use"],
                    }
                )

        return results
    finally:
        db.close()

def get_wanikani_headers():
    return {
        "Authorization": f"Bearer {WANIKANI_API_TOKEN}",
        "Wanikani-Revision": WANIKANI_API_REVISION,
    }


def fetch_all_subjects():
    if not WANIKANI_API_TOKEN:
        raise ValueError("WANIKANI_API_TOKEN is missing.")

    all_subjects = []
    next_url = WANIKANI_SUBJECTS_URL + "?types=vocabulary,kanji"

    while next_url:
        response = requests.get(next_url, headers=get_wanikani_headers(), timeout=30)
        response.raise_for_status()

        payload = response.json()
        all_subjects.extend(payload.get("data", []))

        pages = payload.get("pages", {})
        next_url = pages.get("next_url")

        if next_url:
            time.sleep(1.1)

    return all_subjects


def save_subjects_to_db(subjects):
    db = SessionLocal()
    saved_count = 0

    try:
        for item in subjects:
            existing = db.query(Subject).filter(Subject.subject_id == item["id"]).first()

            characters = item.get("data", {}).get("characters")
            level = item.get("data", {}).get("level")
            object_name = item.get("object")

            if existing:
                existing.object = object_name
                existing.subject_type = object_name
                existing.characters = characters
                existing.level = level
                existing.data_json = json.dumps(item, ensure_ascii=False)
            else:
                db.add(
                    Subject(
                        subject_id=item["id"],
                        object=object_name,
                        subject_type=object_name,
                        characters=characters,
                        level=level,
                        data_json=json.dumps(item, ensure_ascii=False),
                    )
                )

            saved_count += 1

        db.commit()
        return saved_count
    finally:
        db.close()


def is_logged_in(request: Request) -> bool:
    return request.session.get("logged_in") is True


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_logged_in(request):
        return RedirectResponse(url="/dashboard", status_code=303)

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": None,
        },
    )


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, password: str = Form(...)):
    if password == APP_PASSWORD and APP_PASSWORD:
        request.session["logged_in"] = True
        return RedirectResponse(url="/dashboard", status_code=303)

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": "Wrong password",
        },
        status_code=400,
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "uploaded_filename": None,
            "preview_lines": [],
            "headers": [],
            "missing_headers": [],
            "validated_rows": [],
            "blocking_error_count": 0,
            "analysis_candidate_count": 0,
            "analysis_results": [],
            "analysis_results_tsv": "",
            "support_candidate_count": 0,
            "support_results": [],
            "sync_message": None,
            "sync_error": None,
            "movement_candidate_count": len(latest_movement_candidates),
        },
    )


@app.get("/content-checker", response_class=HTMLResponse)
async def content_checker(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "content_checker.html",
        {
            "request": request,
            "input_text": "",
            "result": None,
        },
    )


@app.post("/content-checker/analyze", response_class=HTMLResponse)
async def analyze_content_checker(request: Request, input_text: str = Form("")):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    result = analyze_new_content_text(input_text)

    return templates.TemplateResponse(
        "content_checker.html",
        {
            "request": request,
            "input_text": input_text,
            "result": result,
        },
    )


# UI handler for collocation CSV upload
@app.post("/upload-collocations-ui", response_class=HTMLResponse)
async def upload_collocations_ui(request: Request, file: UploadFile = File(...)):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    content = await file.read()
    text = content.decode("utf-8-sig", errors="replace")
    validation = validate_collocations_csv_text(text)
    collocation_error = None
    collocation_results = []

    if not latest_movement_candidates:
        collocation_error = "Upload a level change CSV first, then upload the collocations CSV."
    else:
        collocation_results = run_collocation_analysis(
            validation["rows"],
            latest_movement_candidates,
            latest_movement_level_map,
        )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "collocations_uploaded_filename": file.filename,
            "collocations_headers": validation["headers"],
            "collocations_missing_headers": validation["missing_headers"],
            "collocations_validated_rows": validation["rows"],
            "collocations_blocking_error_count": validation["blocking_error_count"],
            "collocation_error": collocation_error,
            "collocation_results": collocation_results,
            "collocation_result_count": len(collocation_results),
            "movement_candidate_count": len(latest_movement_candidates),
        },
    )


@app.post("/upload", response_class=HTMLResponse)
async def upload_csv(request: Request, file: UploadFile = File(...)):
    global latest_movement_candidates
    global latest_movement_level_map

    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    raw_bytes = await file.read()
    text = raw_bytes.decode("utf-8", errors="replace")

    validation = validate_csv_text(text)
    analysis_candidates = get_context_candidates(validation["rows"])
    collocation_candidates = get_collocation_candidates(validation["rows"])
    support_candidates = get_support_candidates(validation["rows"])
    movement_candidates = get_support_candidates(validation["rows"])
    movement_level_map = build_movement_level_map(movement_candidates)
    latest_movement_candidates = collocation_candidates
    latest_movement_level_map = movement_level_map
    analysis_results = run_basic_analysis(analysis_candidates, movement_level_map)
    support_results = run_support_content_analysis(support_candidates, movement_level_map)

    safe_name = file.filename.rsplit(".", 1)[0]
    export_filename = f"{safe_name}-analysis-results.csv"
    export_path = EXPORT_DIR / export_filename

    csv_text = build_analysis_results_csv(analysis_results)
    export_path.write_text(csv_text, encoding="utf-8-sig")

    request.session["latest_export_filename"] = export_filename

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "uploaded_filename": file.filename,
            "preview_lines": text.splitlines()[:5],
            "headers": validation["headers"],
            "missing_headers": validation["missing_headers"],
            "validated_rows": validation["rows"],
            "blocking_error_count": validation["blocking_error_count"],
            "analysis_candidate_count": len(analysis_candidates),
            "analysis_results": analysis_results,
            "analysis_results_tsv": build_analysis_results_tsv(analysis_results),
            "support_candidate_count": len(support_candidates),
            "support_results": support_results,
            "sync_message": None,
            "sync_error": None,
            "movement_candidate_count": len(latest_movement_candidates),
        },
    )


@app.post("/sync-subjects", response_class=HTMLResponse)
async def sync_subjects(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    try:
        subjects = fetch_all_subjects()
        saved_count = save_subjects_to_db(subjects)
        sync_message = (
            f"Fetched {len(subjects)} vocabulary and kanji subjects from WaniKani API "
            f"and saved {saved_count} rows to SQLite."
        )
        sync_error = None
    except Exception as e:
        sync_message = None
        sync_error = str(e)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "uploaded_filename": None,
            "preview_lines": [],
            "headers": [],
            "missing_headers": [],
            "validated_rows": [],
            "blocking_error_count": 0,
            "analysis_candidate_count": 0,
            "analysis_results": [],
            "analysis_results_tsv": "",
            "support_candidate_count": 0,
            "support_results": [],
            "sync_message": sync_message,
            "sync_error": sync_error,
        },
    )


@app.post("/export-results")
async def export_results(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    export_filename = request.session.get("latest_export_filename")
    if not export_filename:
        return RedirectResponse(url="/dashboard", status_code=303)

    export_path = EXPORT_DIR / export_filename
    if not export_path.exists():
        return RedirectResponse(url="/dashboard", status_code=303)

    return FileResponse(
        path=export_path,
        media_type="text/csv; charset=utf-8",
        filename=export_filename,
    )


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)
