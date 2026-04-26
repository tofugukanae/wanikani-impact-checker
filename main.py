from os import getenv
from pathlib import Path
import csv
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

REQUIRED_HEADERS = ["characters", "subject_type", "current_level", "new_level"]
ALLOWED_SUBJECT_TYPES = {"vocabulary", "kanji", "radical"}
SUPPORTED_SUBJECT_TYPES = {"vocabulary"}

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

            current_level = None
            new_level = None
            resolved_subject_id = None
            db_level = None

            if not characters or not subject_type or not current_level_raw or not new_level_raw:
                status = "missing_field"
                message = "One or more required fields are empty."
            elif subject_type not in ALLOWED_SUBJECT_TYPES:
                status = "invalid_subject_type"
                message = "subject_type must be vocabulary, kanji, or radical."
            else:
                try:
                    current_level = int(current_level_raw)
                    new_level = int(new_level_raw)

                    if not (1 <= current_level <= 60 and 1 <= new_level <= 60):
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

            if status == "ok":
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

            if status == "ok":
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


def get_analysis_candidates(validated_rows):
    candidates = []

    for row in validated_rows:
        if (
            row["status"] == "ok"
            and row["subject_type"] == "vocabulary"
            and row["resolved_subject_id"]
        ):
            candidates.append(row)

    return candidates


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

    if match_method == "exact_substring_longest":
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


def highlight_sentence_ja(sentence_ja: str, changed_characters: str, match_method: str):
    if not sentence_ja or not changed_characters:
        return sentence_ja

    if match_method == "exact_substring_longest":
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

    if match_method == "exact_substring_longest":
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

def run_basic_analysis(candidates):
    db = SessionLocal()
    results = []

    try:
        vocabulary_subjects = db.query(Subject).filter(Subject.subject_type == "vocabulary").all()

        for candidate in candidates:
            changed_subject_id = candidate["resolved_subject_id"]
            changed_characters = candidate["characters"]
            old_level = int(candidate["current_level"])
            new_level = int(candidate["new_level"])

            changed_subject = (
                db.query(Subject)
                .filter(Subject.subject_id == changed_subject_id)
                .first()
            )
            wk_parts_of_speech = extract_parts_of_speech(changed_subject) if changed_subject else []

            for subject in vocabulary_subjects:
                if subject.subject_id == changed_subject_id:
                    continue

                used_in_level = subject.level
                context_sentences = extract_context_sentences(subject)

                for sentence in context_sentences:
                    sentence_ja_raw = sentence.get("ja") or ""
                    sentence_en_raw = sentence.get("en") or ""

                    if not changed_characters:
                        continue

                    match_method = None

                    if changed_characters in sentence_ja_raw:
                        if sentence_has_longer_competing_match(
                            sentence_ja_raw,
                            changed_characters,
                            vocabulary_subjects,
                        ):
                            continue
                        match_method = "exact_substring_longest"
                    else:
                        match_method = sentence_matches_candidate_with_sudachi(
                            sentence_ja_raw,
                            changed_characters,
                        )

                    if not match_method:
                        continue

                    result_type = None

                    if old_level <= used_in_level and new_level > used_in_level:
                        result_type = "newly_broken"
                    elif old_level > used_in_level and new_level > used_in_level:
                        result_type = "already_broken"

                    if not result_type:
                        continue

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
                            "changed_item_display": f"{changed_characters} ({old_level} → {new_level})",
                            "old_level": old_level,
                            "new_level": new_level,
                            "used_in_subject_id": subject.subject_id,
                            "used_in_characters": subject.characters,
                            "used_in_level": used_in_level,
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

def get_wanikani_headers():
    return {
        "Authorization": f"Bearer {WANIKANI_API_TOKEN}",
        "Wanikani-Revision": WANIKANI_API_REVISION,
    }


def fetch_all_subjects():
    if not WANIKANI_API_TOKEN:
        raise ValueError("WANIKANI_API_TOKEN is missing.")

    all_subjects = []
    next_url = WANIKANI_SUBJECTS_URL + "?types=vocabulary"

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
            "sync_message": None,
            "sync_error": None,
        },
    )


@app.post("/upload", response_class=HTMLResponse)
async def upload_csv(request: Request, file: UploadFile = File(...)):
    if not is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)

    raw_bytes = await file.read()
    text = raw_bytes.decode("utf-8", errors="replace")

    validation = validate_csv_text(text)
    analysis_candidates = get_analysis_candidates(validation["rows"])
    analysis_results = run_basic_analysis(analysis_candidates)

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
            "sync_message": None,
            "sync_error": None,
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
            f"Fetched {len(subjects)} vocabulary subjects from WaniKani API "
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
