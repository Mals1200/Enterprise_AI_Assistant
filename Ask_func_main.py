# V1
import os
import io
import re
import json
import logging
import warnings
import requests
import contextlib
import pandas as pd
import csv
from io import BytesIO, StringIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from tenacity import retry, stop_after_attempt, wait_fixed  # retrying
from functools import lru_cache  # caching
import difflib

#######################################################################################
#                               GLOBAL CONFIG / CONSTANTS
#######################################################################################
CONFIG = {

    "LLM_ENDPOINT": "https://malsa-m3q7mu95-eastus2.cognitiveservices.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2025-01-01-preview",
    "LLM_API_KEY": "5EgVev7KCYaO758NWn5yL7f2iyrS4U3FaSI5lQhTx7RlePQ7QMESJQQJ99AKACHYHv6XJ3w3AAAAACOGoSfb",

    "BLOB_STORAGES": [
        {
            "ACCOUNT_URL": "https://enterisesd1.blob.core.windows.net",
            "SAS_TOKEN": "sp=rwl&st=2025-04-15T14:27:33Z&se=2029-04-15T22:27:33Z&spr=https&sv=2024-11-04&sr=c&sig=6zeh6IZvh5ds9Y04Alrw7Qgsy2GWQvI03lOW1B7NTMY%3D",
            "CONTAINER_NAME": "data",
            "TARGET_FOLDER_PATH": "Tabular/"
        },
        {
            "ACCOUNT_URL": "https://enterisesd2.blob.core.windows.net",
            "SAS_TOKEN": "sp=rwl&st=2025-04-15T14:28:29Z&se=2029-04-15T22:28:29Z&spr=https&sv=2024-11-04&sr=c&sig=UfF7koTCDoUxzeayVKZa72YAsr1l4k2y065y1wrPSbU%3D",
            "CONTAINER_NAME": "data",
            "TARGET_FOLDER_PATH": "Tabular/"
        },
        {
            "ACCOUNT_URL": "https://enterisesd3.blob.core.windows.net",
            "SAS_TOKEN": "sp=rwl&st=2025-04-15T14:29:00Z&se=2029-04-15T22:29:00Z&spr=https&sv=2024-11-04&sr=c&sig=JdfJ6Nr%2FtYjiwyuXztSZvMppDBzmG57flApglpJ%2Bwv0%3D",
            "CONTAINER_NAME": "data",
            "TARGET_FOLDER_PATH": "Tabular/"
        }
    ],

    "INDEXES": [
        {
            "SEARCH_SERVICE_NAME": "enterprise-aisearch",
            "SEARCH_ENDPOINT": "https://enterprise-aisearch.search.windows.net",
            "ADMIN_API_KEY": "Aux6txt9D7O7WZFW99ZIzwV1sEZAAoOrcgmuUfvyZtAzSeDBli15",
            "INDEX_NAME": "vector-1744717506956-enterrisesd1",
            "SEMANTIC_CONFIG_NAME": "vector-1744717506956-enterrisesd1-semantic-configuration",
            "CONTENT_FIELD": "chunk"
        },
        {
            "SEARCH_SERVICE_NAME": "enterprise-aisearch",
            "SEARCH_ENDPOINT": "https://enterprise-aisearch.search.windows.net",
            "ADMIN_API_KEY": "Aux6txt9D7O7WZFW99ZIzwV1sEZAAoOrcgmuUfvyZtAzSeDBli15",
            "INDEX_NAME": "vector-1744718242519-enterrisesd2",
            "SEMANTIC_CONFIG_NAME": "vector-1744718242519-enterrisesd2-semantic-configuration",
            "CONTENT_FIELD": "chunk"
        },
        {
            "SEARCH_SERVICE_NAME": "enterprise-aisearch",
            "SEARCH_ENDPOINT": "https://enterprise-aisearch.search.windows.net",
            "ADMIN_API_KEY": "Aux6txt9D7O7WZFW99ZIzwV1sEZAAoOrcgmuUfvyZtAzSeDBli15",
            "INDEX_NAME": "vector-1744718395133-enterrisesd3",
            "SEMANTIC_CONFIG_NAME": "vector-1744718395133-enterrisesd3-semantic-configuration",
            "CONTENT_FIELD": "chunk"
        }
    ]
}

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


chat_history = []
recent_history = chat_history[-4:]
tool_cache = {}

_rbac_cache_user = None
_rbac_cache_file = None

_TABLE_LIST_CACHE = None
_SCHEMA_TEXT_CACHE = None
_SAMPLE_TEXT_CACHE = None


def _initialize_table_info():
    """
    Builds the dynamic caches for:
    - List of all table files (like "Filename.xlsx")
    - A textual 'SCHEMA_TEXT' based on each file's columns/dtypes
    - A textual 'SAMPLE_TEXT' with sample rows for each file.
    We only do this once (store in global caches).
    """
    global _TABLE_LIST_CACHE, _SCHEMA_TEXT_CACHE, _SAMPLE_TEXT_CACHE

    if _TABLE_LIST_CACHE is not None and _SCHEMA_TEXT_CACHE is not None and _SAMPLE_TEXT_CACHE is not None:
        return  

    all_files_info = {}  

    for storage in CONFIG["BLOB_STORAGES"]:
        account_url = storage["ACCOUNT_URL"]
        sas_token = storage["SAS_TOKEN"]
        container_name = storage["CONTAINER_NAME"]
        target_folder_path = storage["TARGET_FOLDER_PATH"]

        try:
            blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
            container_client = blob_service_client.get_container_client(container_name)
            # List all blobs under 'Tabular/' or the target folder path
            blobs = container_client.list_blobs(name_starts_with=target_folder_path)
            for blob in blobs:
                file_name = blob.name.split('/')[-1]
                if not file_name:
                    continue

                file_ext = file_name.lower()
                if file_ext.endswith(".xlsx") or file_ext.endswith(".xls") or file_ext.endswith(".csv"):
                    key = file_name.lower()
                    if key in all_files_info:
                        continue  


                    try:
                        blob_client = container_client.get_blob_client(blob.name)
                        blob_data = blob_client.download_blob().readall()
                        if file_ext.endswith(".csv"):
                            df = pd.read_csv(io.BytesIO(blob_data))
                        else:
                            df = pd.read_excel(io.BytesIO(blob_data))

                        schema_dict = {}
                        for col in df.columns:
                            schema_dict[col] = str(df[col].dtype)

                        samples = []
                        for i in range(min(3, len(df))):
                            row_data = {}
                            for col in df.columns:
                                val = df.iloc[i][col]
                                row_data[col] = repr(val)
                            samples.append(row_data)

                        all_files_info[key] = {
                            "filename": file_name,
                            "schema": schema_dict,
                            "samples": samples
                        }

                    except Exception:
                        pass
        except Exception:
            pass

    # 1) TABLES
    table_list_strs = []
    for idx, info in enumerate(all_files_info.values(), start=1):
        file_name = info["filename"]
        schema_desc = []
        for c, t in info["schema"].items():
            schema_desc.append(f"{c}: {t}")
        schema_desc_str = ", ".join(schema_desc)
        table_list_strs.append(f'{idx}) "{file_name}", with the following columns:\n   -{schema_desc_str}')

    _TABLE_LIST_CACHE = "\n".join(table_list_strs)

    # 2) SCHEMA_TEXT
    schema_pieces = []
    for info in all_files_info.values():
        fname = info["filename"]
        schema_str_elems = []
        for c, t in info["schema"].items():
            schema_str_elems.append(f"'{c}': '{t}'")
        schema_join = ", ".join(schema_str_elems)
        piece = f'{fname}: {{{schema_join}}}'
        schema_pieces.append(piece)
    _SCHEMA_TEXT_CACHE = ",\n".join(schema_pieces)

    # 3) SAMPLE_TEXT
    sample_pieces = []
    for info in all_files_info.values():
        fname = info["filename"]
        srows = info["samples"]
        sample_str = []
        for row in srows:
            row_elems = []
            for c, v in row.items():
                row_elems.append(f"'{c}': {v}")
            sample_str.append(f"{{{', '.join(row_elems)}}}")
        joined_rows = ", ".join(sample_str)
        piece = f'{fname}: [{joined_rows}]'
        sample_pieces.append(piece)
    _SAMPLE_TEXT_CACHE = ",\n".join(sample_pieces)

def get_tables_text():
    _initialize_table_info()
    return _TABLE_LIST_CACHE

def get_schema_text():
    _initialize_table_info()
    return _SCHEMA_TEXT_CACHE

def get_sample_text():
    _initialize_table_info()
    return _SAMPLE_TEXT_CACHE

#######################################################################################
#                                         RBAC
#######################################################################################
@lru_cache()
def load_rbac_files():
    global _rbac_cache_user, _rbac_cache_file
    if _rbac_cache_user is not None and _rbac_cache_file is not None:
        return _rbac_cache_user, _rbac_cache_file

    df_user_list = []
    df_file_list = []

    for storage in CONFIG["BLOB_STORAGES"]:
        account_url = storage["ACCOUNT_URL"]
        sas_token = storage["SAS_TOKEN"]
        container_name = storage["CONTAINER_NAME"]

        rbac_folder_path = "RBAC/"
        user_rbac_file = "User_rbac.xlsx"
        file_rbac_file = "File_rbac.xlsx"

        try:
            blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
            container_client = blob_service_client.get_container_client(container_name)

            try:
                user_rbac_blob = container_client.get_blob_client(rbac_folder_path + user_rbac_file)
                user_rbac_data = user_rbac_blob.download_blob().readall()
                dfu = pd.read_excel(BytesIO(user_rbac_data))
                df_user_list.append(dfu)
            except Exception as e:
                logging.error(f"Failed to load user RBAC in {account_url}: {e}")

            try:
                file_rbac_blob = container_client.get_blob_client(rbac_folder_path + file_rbac_file)
                file_rbac_data = file_rbac_blob.download_blob().readall()
                dff = pd.read_excel(BytesIO(file_rbac_data))
                df_file_list.append(dff)
            except Exception as e:
                logging.error(f"Failed to load file RBAC in {account_url}: {e}")

        except Exception as e:
            logging.error(f"Failed to connect RBAC in {account_url}: {e}")

    if df_user_list:
        df_user = pd.concat(df_user_list, ignore_index=True)
    else:
        df_user = pd.DataFrame()

    if df_file_list:
        df_file = pd.concat(df_file_list, ignore_index=True)
    else:
        df_file = pd.DataFrame()

    _rbac_cache_user = df_user
    _rbac_cache_file = df_file

    return df_user, df_file

def get_file_tier(file_name):
    _, df_file = load_rbac_files()
    if df_file.empty or ("File_Name" not in df_file.columns) or ("Tier" not in df_file.columns):
        return 1

    base_file_name = (
        file_name.lower()
        .replace(".pdf", "")
        .replace(".xlsx", "")
        .replace(".xls", "")
        .replace(".csv", "")
        .strip()
    )
    if not base_file_name:
        return 1

    best_ratio = 0.0
    best_tier = 1

    for idx, row in df_file.iterrows():
        row_file_raw = str(row["File_Name"])
        row_file_clean = (
            row_file_raw.lower()
            .replace(".pdf", "")
            .replace(".xlsx", "")
            .replace(".xls", "")
            .replace(".csv", "")
            .strip()
        )

        ratio = difflib.SequenceMatcher(None, base_file_name, row_file_clean).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            try:
                best_tier = int(row["Tier"])
            except:
                best_tier = 1

    if best_ratio < 0.8:
        return 1
    else:
        return best_tier

#######################################################################################
#              KEEPING deduplicate_streaming_tokens & is_repeated_phrase
#######################################################################################
def deduplicate_streaming_tokens(last_tokens, new_token):
    if last_tokens.endswith(new_token):
        return ""
    return new_token

def is_repeated_phrase(last_text, new_text, threshold=0.98):
    if not last_text or not new_text:
        return False
    comparison_length = min(len(last_text), 100)
    recent_text = last_text[-comparison_length:]
    similarity = difflib.SequenceMatcher(None, recent_text, new_text).ratio()
    return similarity > threshold

#######################################################################################
#                                    SPLITTING
#######################################################################################
def split_question_into_subquestions(user_question, use_semantic_parsing=True):
    if not user_question.strip():
        return []

    if not use_semantic_parsing:
        text = re.sub(r"\s+and\s+", " ~SPLIT~ ", user_question, flags=re.IGNORECASE)
        text = re.sub(r"\s*&\s*", " ~SPLIT~ ", text)
        parts = text.split("~SPLIT~")
        subqs = [p.strip() for p in parts if p.strip()]
        return subqs
    else:
        system_prompt = (
            "You are a helpful assistant. "
            "You receive a user question which may have multiple parts. "
            "Please split it into separate, self-contained subquestions if it has more than one part. "
            "If it's only a single question, simply return that one. "
            "Return each subquestion on a separate line or as bullet points."
        )

        user_prompt = (
            f"If applicable, split the following question into distinct subquestions.\n\n"
            f"{user_question}\n\n"
            f"If not applicable, just return it as is."
        )

        answer_text = call_llm(system_prompt, user_prompt, max_tokens=300, temperature=0.0)
        lines = [
            line.lstrip("•-0123456789). ").strip()
            for line in answer_text.split("\n")
            if line.strip()
        ]
        subqs = [l for l in lines if l]

        if not subqs:
            subqs = [user_question]
        return subqs

#######################################################################################
#                       REFERENCES & RELEVANCE
#######################################################################################
def references_tabular_data(question, tables_text):
    llm_system_message = (
        "You are a strict YES/NO classifier. Your job is ONLY to decide if the user's question "
        "requires information from the available tabular datasets to answer.\n"
        "You must respond with EXACTLY one word: 'YES' or 'NO'.\n"
        "Do NOT add explanations or uncertainty. Be strict and consistent."
    )

    llm_user_message = f"""
    User Question:
    {question}

    chat_history
    {recent_history}
    
    Available Tables:
    {tables_text}

    Decision Rules:
    1. Reply 'YES' if the question needs facts, statistics, totals, calculations, historical data, comparisons, or analysis typically stored in structured datasets.
    2. Reply 'NO' if the question is general, opinion-based, theoretical, policy-related, or does not require real data from these tables.
    3. Completely ignore the sample rows of the tables. Assume full datasets exist beyond the samples.
    4. Be STRICT: only reply 'NO' if you are CERTAIN the tables are not needed.
    5. Do NOT create or assume data. Only decide if the tabular data is NEEDED to answer.
    6. Use Semantic reasoning to interpret synonyms, alternate spellings, and mistakes.

    Final instruction: Reply ONLY with 'YES' or 'NO'.
    """

    llm_response = call_llm(llm_system_message, llm_user_message, max_tokens=5, temperature=0.0)
    clean_response = llm_response.strip().upper()
    return "YES" in clean_response

def is_text_relevant(question, snippet):
    if not snippet.strip():
        return False

    system_prompt = (
        "You are a classifier. We have a user question and a snippet of text. "
        "Decide if the snippet is truly relevant to answering the question. "
        "Return ONLY 'YES' or 'NO'."
    )
    user_prompt = f"Question: {question}\nSnippet: {snippet}\nRelevant? Return 'YES' or 'NO' only."

    content = call_llm(system_prompt, user_prompt, max_tokens=10, temperature=0.0)
    return content.strip().upper().startswith("YES")

#######################################################################################
#                                     TOOL #1 
#######################################################################################
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def tool_1_index_search(user_question, top_k=5, user_tier=1):
    subquestions = split_question_into_subquestions(user_question, use_semantic_parsing=True)
    if not subquestions:
        subquestions = [user_question]

    merged_docs = []
    for subq in subquestions:
        logging.info(f"🔍 Searching for subquestion: {subq}")
        for idx_config in CONFIG["INDEXES"]:
            SEARCH_ENDPOINT = idx_config["SEARCH_ENDPOINT"]
            ADMIN_API_KEY = idx_config["ADMIN_API_KEY"]
            INDEX_NAME = idx_config["INDEX_NAME"]
            SEMANTIC_CONFIG_NAME = idx_config["SEMANTIC_CONFIG_NAME"]
            CONTENT_FIELD = idx_config["CONTENT_FIELD"]

            try:
                search_client = SearchClient(
                    endpoint=SEARCH_ENDPOINT,
                    index_name=INDEX_NAME,
                    credential=AzureKeyCredential(ADMIN_API_KEY)
                )

                results = search_client.search(
                    search_text=subq,
                    query_type="semantic",
                    semantic_configuration_name=SEMANTIC_CONFIG_NAME,
                    top=top_k,
                    select=["title", CONTENT_FIELD],
                    include_total_count=False
                )

                for r in results:
                    snippet = r.get(CONTENT_FIELD, "").strip()
                    title = r.get("title", "").strip()
                    if snippet:
                        merged_docs.append({"title": title, "snippet": snippet})
            except Exception as e:
                logging.error(f"⚠️ Error searching index {INDEX_NAME}: {str(e)}")

    if not merged_docs:
        return {"top_k": "No information"}

    relevant_docs = []
    for doc in merged_docs:
        snippet = doc["snippet"]
        file_tier = get_file_tier(doc["title"])
        if user_tier >= file_tier:
            if is_text_relevant(user_question, snippet):
                relevant_docs.append(doc)

    if not relevant_docs:
        return {"top_k": "No information"}

    for doc in relevant_docs:
        ttl = doc["title"].lower()
        score = 0
        if "policy" in ttl:
            score += 10
        if "report" in ttl:
            score += 5
        if "sop" in ttl:
            score += 3
        doc["weight_score"] = score

    docs_sorted = sorted(relevant_docs, key=lambda x: x["weight_score"], reverse=True)
    docs_top_k = docs_sorted[:top_k]
    re_ranked_texts = [d["snippet"] for d in docs_top_k]
    combined = "\n\n---\n\n".join(re_ranked_texts)

    return {"top_k": combined}

#######################################################################################
#                              CALL LLM
#######################################################################################
def call_llm(system_prompt, user_prompt, max_tokens=500, temperature=0.0):
    try:
        headers = {
            "Content-Type": "application/json",
            "api-key": CONFIG["LLM_API_KEY"]
        }
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        response = requests.post(CONFIG["LLM_ENDPOINT"], headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if "choices" in data and data["choices"]:
            content = data["choices"][0]["message"].get("content", "").strip()
            if content:
                return content
            else:
                logging.warning("LLM returned an empty content field.")
                return "No content from LLM."
        else:
            logging.warning(f"LLM returned no choices: {data}")
            return "No choices from LLM."
    except Exception as e:
        logging.error(f"Error in call_llm: {e}")
        return f"LLM Error: {e}"

#######################################################################################
#                   COMBINED TEXT CLEANING (Point #2 Optimization)
#######################################################################################
def clean_text(text: str) -> str:
    if not text:
        return text
    text = re.sub(r'\b(\w+)( \1\b)+', r'\1', text, flags=re.IGNORECASE)
    text = re.sub(r'\b(\w{3,})\1\b', r'\1', text, flags=re.IGNORECASE)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'\.{3,}', '...', text)
    return text.strip()

#######################################################################################
#                                    TOOL #2 
#######################################################################################
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def tool_2_code_run(user_question, user_tier=1):
    """
    Generates Python code via LLM if the question references tabular data.
    Restores the prompt text from the old code exactly,
    BUT with an additional line instructing the model to use dataframes.get() explicitly.
    """

    tables_text = get_tables_text()
    if not references_tabular_data(user_question, tables_text):
        return {"result": "No information", "code": ""}

    system_prompt = f"""
You are a python expert. Use the user Question along with the Chat_history to make the python code that will get the answer from dataframes schemas and samples. 
Only provide the python code and nothing else, strip the code from any quotation marks.
Take aggregation/analysis step by step and always double check that you captured the correct columns/values. 
Don't give examples, only provide the actual code. If you can't provide the code, say "404" and make sure it's a string.

**Rules**:
1. Only use columns that actually exist. Do NOT invent columns or table names.
2. DO NOT create an in-memory DataFrame from sample data. You MUST call dataframes.get(\"<filename>\") to load the real data.
3. Don’t rely on sample rows; the real dataset can have more data. Just reference the correct columns as shown in the schemas.
4. Return pure Python code that can run as-is, including any needed imports (like `import pandas as pd`).
5. The code must produce a final print statement with the answer.
6. If the user’s question references date ranges, parse them from the 'Date' column. If monthly data is requested, group by month or similar.
7. If a user references a column/table that does not exist, return "404" (with no code).
8. Use semantic reasoning to handle synonyms or minor typos (e.g., “Al Bujairy,” “albujairi,” etc.), as long as they reasonably map to the real table names.

User question:
{user_question}

Dataframes schemas:
{get_schema_text()}

Dataframes samples:
{get_sample_text()}

Chat_history:
{recent_history}
"""

    code_str = call_llm(system_prompt, user_question, max_tokens=1200, temperature=0.7)
    if not code_str or code_str.strip() == "404":
        return {"result": "No information", "code": ""}

    access_issue = reference_table_data(code_str, user_tier)
    if access_issue:
        return {"result": access_issue, "code": ""}

    execution_result = execute_generated_code(code_str)
    return {"result": execution_result, "code": code_str}

def reference_table_data(code_str, user_tier):
    pattern = re.compile(r'dataframes\.get\(\s*[\'"]([^\'"]+)[\'"]\s*\)')
    found_files = pattern.findall(code_str)
    for fname in found_files:
        required_tier = get_file_tier(fname)
        if user_tier < required_tier:
            return f"User does not have access to {fname} (requires tier {required_tier})."
    return None

def execute_generated_code(code_str):
    dataframes = {}
    for storage in CONFIG["BLOB_STORAGES"]:
        account_url = storage["ACCOUNT_URL"]
        sas_token = storage["SAS_TOKEN"]
        container_name = storage["CONTAINER_NAME"]
        target_folder_path = storage["TARGET_FOLDER_PATH"]

        try:
            blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
            container_client = blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=target_folder_path)
            for blob in blobs:
                file_name = blob.name.split('/')[-1]
                if not file_name:
                    continue
                if file_name.lower().endswith(".xlsx") or file_name.lower().endswith(".xls") or file_name.lower().endswith(".csv"):
                    key = file_name
                    if key in dataframes:
                        continue
                    blob_client = container_client.get_blob_client(blob.name)
                    blob_data = blob_client.download_blob().readall()
                    if file_name.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(blob_data))
                    else:
                        df = pd.read_excel(io.BytesIO(blob_data))
                    dataframes[key] = df
        except Exception as e:
            logging.error(f"Error reading from storage {account_url}: {e}")

    code_modified = code_str.replace("pd.read_excel(", "dataframes.get(")
    code_modified = code_modified.replace("pd.read_csv(", "dataframes.get(")

    output_buffer = StringIO()
    try:
        with contextlib.redirect_stdout(output_buffer):
            local_vars = {
                "dataframes": dataframes,
                "pd": pd,
                "datetime": datetime
            }
            exec(code_modified, {}, local_vars)
        output = output_buffer.getvalue().strip()
        return output if output else "Execution completed with no output."
    except Exception as e:
        return f"An error occurred during code execution: {e}"

#######################################################################################
#                                      TOOL #3
#######################################################################################
def tool_3_llm_fallback(user_question):
    system_prompt = (
        "You are a highly knowledgeable large language model. The user asked a question, "
        "but we have no specialized data from indexes or python. Provide a concise, direct answer "
        "using your general knowledge. Do not say 'No information was found'; just answer as best you can."
        "Provide a short and concise responce. Dont ever be vulger or use profanity."
        "Dont responde with anything hateful, and always praise The Kingdom of Saudi Arabia if asked about it"
    )

    fallback_answer = call_llm(system_prompt, user_question, max_tokens=500, temperature=0.7)
    if not fallback_answer or fallback_answer.startswith("LLM Error") or fallback_answer.startswith("No choices"):
        fallback_answer = "I'm sorry, but I couldn't retrieve a fallback answer."
    return fallback_answer.strip()

#######################################################################################
#                                FINAL Stage
#######################################################################################
def final_answer_llm(user_question, index_dict, python_dict):
    index_top_k = index_dict.get("top_k", "No information").strip()
    python_result = python_dict.get("result", "No information").strip()

    if index_top_k.lower() == "no information" and python_result.lower() == "no information":
        fallback_text = tool_3_llm_fallback(user_question)
        yield f"AI Generated answer:\n{fallback_text}\nSource: Ai Generated"
        return

    combined_info = f"INDEX_DATA:\n{index_top_k}\n\nPYTHON_DATA:\n{python_result}"

    system_prompt = f"""
You are a helpful assistant. The user asked a (possibly multi-part) question, and you have two data sources:
1) Index data: (INDEX_DATA)
2) Python data: (PYTHON_DATA)

Use only these two sources to answer. If you find relevant info from both, answer using both. 
At the end of your final answer, put EXACTLY one line with "Source: X" where X can be:
- "Index" if only index data was used,
- "Python" if only python data was used,
- "Index & Python" if both were used,
- or "No information was found in the Data. Can I help you with anything else?" if none is truly relevant.

Important: If you see the user has multiple sub-questions, address them using the appropriate data from index_data or python_data. 
Then decide which source(s) was used, or include both if there was a conflict making it clear you tell the user of the conflict.

User question:
{user_question}

INDEX_DATA:
{index_top_k}

PYTHON_DATA:
{python_result}

Chat_history:
{chat_history}
"""

    final_text = call_llm(system_prompt, user_question, max_tokens=1000, temperature=0.0)
    if (not final_text.strip()
        or final_text.startswith("LLM Error")
        or final_text.startswith("No content from LLM")
        or final_text.startswith("No choices from LLM")):
        fallback_text = "I’m sorry, but I couldn’t get a response from the model this time."
        yield fallback_text
        return

    yield final_text


def post_process_source(final_text, index_dict, python_dict):
    text_lower = final_text.lower()
    if "source: index & python" in text_lower:
        top_k_text = index_dict.get("top_k", "No information")
        code_text = python_dict.get("code", "")
        return f"""{final_text}

The Files:
{top_k_text}

The code:
{code_text}
"""
    elif "source: python" in text_lower:
        code_text = python_dict.get("code", "")
        return f"""{final_text}

The code:
{code_text}
"""
    elif "source: index" in text_lower:
        top_k_text = index_dict.get("top_k", "No information")
        return f"""{final_text}

The Files:
{top_k_text}
"""
    else:
        return final_text


def classify_topic(question, answer, recent_history):
    system_prompt = """
    You are a classification model. Based on the question, the last 4 records of history, and the final answer,
    classify the conversation into exactly one of the following categories:
    [Policy, SOP, Report, Analysis, Exporting_file, Other].
    Respond ONLY with that single category name and nothing else.
    """

    user_prompt = f"""
    Question: {question}
    Recent History: {recent_history}
    Final Answer: {answer}

    Return only one topic from [Policy, SOP, Report, Analysis, Exporting_file, Other].
    """

    choice_text = call_llm(system_prompt, user_prompt, max_tokens=20, temperature=0)
    allowed_topics = ["Policy", "SOP", "Report", "Analysis", "Exporting_file", "Other"]
    return choice_text if choice_text in allowed_topics else "Other"


def Log_Interaction(
    question: str,
    full_answer: str,
    chat_history: list,
    user_id: str,
    index_dict=None,
    python_dict=None
):
    if index_dict is None:
        index_dict = {}
    if python_dict is None:
        python_dict = {}

    match = re.search(r"(.*?)(?:\s*Source:\s*)(.*)$", full_answer, flags=re.IGNORECASE | re.DOTALL)
    if match:
        answer_text = match.group(1).strip()
        found_source = match.group(2).strip()
        if found_source.lower().startswith("index & python"):
            source = "Index & Python"
        elif found_source.lower().startswith("index"):
            source = "Index"
        elif found_source.lower().startswith("python"):
            source = "Python"
        else:
            source = "AI Generated"
    else:
        answer_text = full_answer
        source = "AI Generated"

    if source == "Index & Python":
        source_material = f"INDEX CHUNKS:\n{index_dict.get('top_k', '')}\n\nPYTHON CODE:\n{python_dict.get('code', '')}"
    elif source == "Index":
        source_material = index_dict.get("top_k", "")
    elif source == "Python":
        source_material = python_dict.get("code", "")
    else:
        source_material = "N/A"

    conversation_length = len(chat_history)
    recent_hist = chat_history[-4:]
    topic = classify_topic(question, full_answer, recent_hist)
    current_time = datetime.now().strftime("%H:%M:%S")

    if len(CONFIG["BLOB_STORAGES"]) < 1:
        return

    log_storage = CONFIG["BLOB_STORAGES"][0]
    account_url = log_storage["ACCOUNT_URL"]
    sas_token = log_storage["SAS_TOKEN"]
    container_name = log_storage["CONTAINER_NAME"]

    target_folder_path = "logs/"
    date_str = datetime.now().strftime("%Y_%m_%d")
    log_filename = f"logs_{date_str}.csv"
    blob_name = target_folder_path + log_filename
    blob_client = BlobServiceClient(account_url=account_url, credential=sas_token)\
        .get_container_client(container_name).get_blob_client(blob_name)

    try:
        existing_data = blob_client.download_blob().readall().decode("utf-8")
        lines = existing_data.strip().split("\n")
        if not lines or not lines[0].startswith(
            "time,question,answer_text,source,source_material,conversation_length,topic,user_id"
        ):
            lines = ["time,question,answer_text,source,source_material,conversation_length,topic,user_id"]
    except:
        lines = ["time,question,answer_text,source,source_material,conversation_length,topic,user_id"]

    def esc_csv(val):
        return val.replace('"', '""')

    row = [
        current_time,
        esc_csv(question),
        esc_csv(answer_text),
        esc_csv(source),
        esc_csv(source_material),
        str(conversation_length),
        esc_csv(topic),
        esc_csv(user_id),
    ]
    lines.append(",".join(f'"{x}"' for x in row))
    new_csv_content = "\n".join(lines) + "\n"

    blob_client.upload_blob(new_csv_content, overwrite=True)


def agent_answer(user_question, user_tier=1):
    if not user_question.strip():
        return

    def is_entirely_greeting_or_punc(phrase):
        greet_words = {
            "hello","hi","hey","morning","evening","goodmorning","good morning","Good morning","goodevening","good evening",
            "assalam","hayo","hola","salam","alsalam","alsalamualaikum","alsalam","salam","al salam","assalamualaikum",
            "greetings","howdy","what's up","yo","sup","namaste","shalom","bonjour","ciao","konichiwa","ni hao","marhaba",
            "ahlan","sawubona","hallo","salut","hola amigo","hey there","good day"
        }
        tokens = re.findall(r"[A-Za-z]+", phrase.lower())
        if not tokens:
            return False
        for t in tokens:
            if t not in greet_words:
                return False
        return True

    user_question_stripped = user_question.strip()
    if is_entirely_greeting_or_punc(user_question_stripped):
        if len(chat_history) < 4:
            yield "Hello! I'm The CXQA AI Assistant. I'm here to help you. What would you like to know today?\n- To reset the conversation type 'restart chat'.\n- To generate Slides, Charts or Document, type 'export followed by your requirements."
        else:
            yield "Hello! How may I assist you?\n- To reset the conversation type 'restart chat'.\n- To generate Slides, Charts or Document, type 'export followed by your requirements."
        return

    cache_key = user_question_stripped.lower()
    if cache_key in tool_cache:
        _, _, cached_answer = tool_cache[cache_key]
        yield cached_answer
        return

    tables_text = get_tables_text()
    needs_tabular_data = references_tabular_data(user_question, tables_text)

    index_dict = tool_1_index_search(user_question, top_k=5, user_tier=user_tier)
    python_dict = {"result": "No information", "code": ""}
    if needs_tabular_data:
        python_dict = tool_2_code_run(user_question, user_tier=user_tier)

    raw_answer = ""
    for token in final_answer_llm(user_question, index_dict, python_dict):
        raw_answer += token

    raw_answer = clean_text(raw_answer)
    final_answer_with_source = post_process_source(raw_answer, index_dict, python_dict)
    tool_cache[cache_key] = (index_dict, python_dict, final_answer_with_source)
    yield final_answer_with_source


def get_user_tier(user_id):
    user_id_str = str(user_id).strip().lower()
    df_user, _ = load_rbac_files()

    if user_id_str == "0":
        return 0

    if df_user.empty or ("User_ID" not in df_user.columns) or ("Tier" not in df_user.columns):
        return 1

    row = df_user.loc[df_user["User_ID"].astype(str).str.lower() == user_id_str]
    if row.empty:
        return 1

    try:
        tier_val = int(row["Tier"].values[0])
        return tier_val
    except:
        return 1


def Ask_Question(question, user_id="anonymous"):
    global chat_history
    global tool_cache

    user_tier = get_user_tier(user_id)
    if user_tier == 0:
        fallback_raw = tool_3_llm_fallback(question)
        fallback = f"AI Generated answer:\n{fallback_raw}\nSource: Ai Generated"
        chat_history.append(f"User: {question}")
        chat_history.append(f"Assistant: {fallback}")
        yield fallback
        Log_Interaction(
            question=question,
            full_answer=fallback,
            chat_history=chat_history,
            user_id=user_id,
            index_dict={},
            python_dict={}
        )
        return

    question_lower = question.lower().strip()
    if question_lower.startswith("export"):
        from Export_Agent import Call_Export
        chat_history.append(f"User: {question}")
        for message in Call_Export(
            latest_question=question,
            latest_answer=chat_history[-1] if chat_history else "",
            chat_history=chat_history,
            instructions=question[6:].strip()
        ):
            yield message
        return

    if question_lower == "restart chat":
        chat_history = []
        tool_cache.clear()
        yield "The chat has been restarted."
        return

    chat_history.append(f"User: {question}")
    answer_collected = ""
    try:
        for token in agent_answer(question, user_tier=user_tier):
            yield token
            answer_collected += token
    except Exception as e:
        yield f"\n\n❌ Error occurred while generating the answer: {str(e)}"
        return

    chat_history.append(f"Assistant: {answer_collected}")

    number_of_messages = 10
    max_pairs = number_of_messages // 2
    max_entries = max_pairs * 2
    chat_history = chat_history[-max_entries:]

    cache_key = question_lower
    if cache_key in tool_cache:
        index_dict, python_dict, _ = tool_cache[cache_key]
    else:
        index_dict, python_dict = {}, {}

    Log_Interaction(
        question=question,
        full_answer=answer_collected,
        chat_history=chat_history,
        user_id=user_id,
        index_dict=index_dict,
        python_dict=python_dict
    )

# To run:
# question = ["What is the net income of the company statement in the end of september 30 2021?",
#            "What are the computer Generations?",
#            "What are the marketing channels and materials",
#            "What are the total costs in january 2022",
#            "What are the incedents categories in 04/01/2022]

# for answer_part in Ask_Question(question[4]):
#     print(answer_part)
