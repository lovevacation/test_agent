import os
import re
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

PAUSE_FILE = "pause.txt"
FAILED_IDS_FILE = "failed_ids.txt"
FAILED_DETAIL_FILE = "failed_details.jsonl"

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
MODEL_NAME = "deepseek-r1:8b"
MAX_WORKERS = int(os.getenv("CLEAN_MAX_WORKERS", "3"))  # 默认2线程

SOFT_SKILL_WHITELIST = {
    "沟通能力", "团队合作能力", "问题解决能力",
    "持续学习能力", "创新能力", "抗压能力", "某种语言能力"
}

SYSTEM_RULE = """
你是岗位结构化信息提取助手。请根据“岗位名称+岗位描述”抽取结构化信息。

【总体要求】
1. 允许合理推断，但不得捏造与岗位无关内容。
2. 缺失信息可填 null 或 []，不要编造。
3. 最后必须“仅输出一个JSON对象”，不要输出解释文字。

【字段含义与抽取口径】
1) career_dir（职业方向，string/null）
   - 含义：岗位所属的大方向，如“后端开发”“数据分析”等。
   - 要求：1个方向；无法判断填 null。

2) job_level（岗位级别，string/null）
   - 含义：岗位经验层级，取值仅允许：低 / 中 / 高
   - 规则：
     - 低：应届、实习、无经验、<=1年
     - 中：2-5年
     - 高：>=5年、架构师、负责人、专家、高级
   - 无法判断填 null。

3) skills（核心技能，二维数组，必为 List[List[str]]）
   - 含义：岗位硬技能要求（编程语言、框架、中间件、数据库、行业软件、方法能力等）。
   - 结构规则（必须遵守）：
     - 同一子数组内 = AND（构成完整知识链）
     - 不同子数组之间 = OR (可会一条知识链或多条）
   - 示例：
     - [["Java","MySQL","Linux"],["Java","PostgreSQL","Linux"]]
   - 若无明确信息输出 []。

4) certificates（证书要求，数组）
   - 含义：岗位要求或优先的资格证书，如“软考中级”“PMP”“CPA”等。
   - 无明确证书要求则输出 []。

5) education_level（学历要求，string/null）
   - 含义：岗位最低学历要求，如“不限/大专/本科/硕士/博士”。
   - 尽量按原文提取；无明确要求填 null。

6) experience_years（经验要求，string/null）
   - 含义：工作经验年限要求，格式 ?<x<?
   - 根据JD原文提取；无明确要求填 null。

7) soft_skills（软技能，数组）
   - 含义：非技术能力项。
   - 只能从以下白名单中选择，禁止输出白名单外内容：
     ["沟通能力","团队合作能力","问题解决能力","持续学习能力","创新能力","抗压能力","某种语言能力"]
   - 无则输出 []。

8) job_tasks（岗位职责，数组）
   - 含义：岗位实际工作内容/职责拆解后的条目列表。
   - 要求：每条是可执行职责短句，去重、去空；无则 []。
   
9) domain_tags（领域标签，数组）【领域技能规则】
   - 用于标记岗位所属行业/学科领域，不是具体工具名。
   - 例：["地理信息","测绘工程","土木工程","生物医药","金融","制造","教育","医疗","法务","供应链"]
   - 要求：2~5个，高层标签，去重；无则 []。

10) open_knowledge（开放知识点，数组）【领域技能规则】
   - 存放“难以标准化但有价值”的小众知识/专业方法/行业术语。
   - 例：["混凝土耐久性优化设计方法","细胞培养基础知识","遥感影像判读","GMP规范理解"]
   - 要求：
     - 保留原始专业表达，不强行改写成通用技术词；
     - 不要放通用词（如“认真负责”“办公软件”）；
     - 1~6条，去重；无则 []。

【输出格式（必须严格一致，且只输出JSON）】
{
  "career_dir": null,
  "job_level": null,
  "skills": [],
  "certificates": [],
  "education_level": null,
  "experience_years": null,
  "soft_skills": [],
  "job_tasks": [],
  "domain_tags": [],
  "open_knowledge": []
}
"""

def get_db():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="career_db",
        user="postgres",
        password="123456"
    )

def safe_json(v):
    if isinstance(v, list):
        return Json(v, dumps=lambda x: json.dumps(x, ensure_ascii=False))
    return None

def infer_job_level(text):
    text = text or ""
    if re.search(r'应届|实习|无经验|1年', text):
        return "低"
    if re.search(r'2年|3年|4年|5年', text):
        return "中"
    if re.search(r'5年|架构师|负责人|专家|高级', text):
        return "高"
    return None

def extract_first_json_object(s: str) -> str:
    s = (s or "").strip()
    if not s:
        raise ValueError("empty response text")

    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)

    start = s.find("{")
    if start < 0:
        raise ValueError("no '{' found in response")

    depth = 0
    end = -1
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break

    if end < 0:
        raise ValueError("no complete JSON object found")
    return s[start:end + 1]

def normalize_skills(skills):
    out = []
    if isinstance(skills, list):
        for g in skills:
            if isinstance(g, list):
                vals = [str(x).strip() for x in g if str(x).strip()]
            elif g is not None:
                vals = [str(g).strip()]
            else:
                vals = []
            vals = list(dict.fromkeys(vals))
            if vals:
                out.append(vals)
    return out

def normalize_list(v):
    if not isinstance(v, list):
        return []
    return list(dict.fromkeys([str(x).strip() for x in v if str(x).strip()]))

def call_llm(job):
    if os.path.exists(PAUSE_FILE):
        return {"id": job["id"], "title": job["title"], "ok": False, "stage": "paused", "error": "pause file detected", "raw": ""}

    prompt = f"""{SYSTEM_RULE}

现在开始抽取，直接输出 JSON：
岗位名称: {job['title']}
岗位描述: {job['description']}
"""
    raw_text = ""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1}
            },
            timeout=1000
        )
        resp.raise_for_status()
        j = resp.json()
        raw_text = (j.get("response") or "").strip()
        if not raw_text:
            raise ValueError("model response is empty")

        data = json.loads(extract_first_json_object(raw_text))

        if not data.get("job_level"):
            data["job_level"] = infer_job_level(job.get("description", ""))

        data["skills"] = normalize_skills(data.get("skills"))
        data["certificates"] = normalize_list(data.get("certificates"))
        data["soft_skills"] = [x for x in normalize_list(data.get("soft_skills")) if x in SOFT_SKILL_WHITELIST]
        data["job_tasks"] = normalize_list(data.get("job_tasks"))
        data["domain_tags"] = normalize_list(data.get("domain_tags"))
        data["open_knowledge"] = normalize_list(data.get("open_knowledge"))

        return {"id": job["id"], "title": job["title"], "ok": True, "data": data}
    except Exception as e:
        return {
            "id": job["id"], "title": job["title"], "ok": False,
            "stage": "llm_or_parse", "error": f"{type(e).__name__}: {e}", "raw": raw_text
        }

def write_failure_files(failed_details):
    uniq_ids = sorted(set(x["id"] for x in failed_details))
    with open(FAILED_IDS_FILE, "w", encoding="utf-8") as f:
        f.write(",".join(map(str, uniq_ids)))

    with open(FAILED_DETAIL_FILE, "w", encoding="utf-8") as f:
        for item in failed_details:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"失败ID文件: {FAILED_IDS_FILE}（{len(uniq_ids)} 条）")
    print(f"失败详情文件: {FAILED_DETAIL_FILE}（{len(failed_details)} 条）")

def main():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
    SELECT id, title, description
    FROM job_position
    WHERE career_dir IS NULL
       OR job_level IS NULL
       OR skills IS NULL
       OR certificates IS NULL
       OR education_level IS NULL
       OR experience_years IS NULL
       OR soft_skills IS NULL
       OR job_tasks IS NULL
       OR domain_tags IS NULL
       OR open_knowledge IS NULL
    ORDER BY id ASC
    """)
    jobs = cursor.fetchall()
    total = len(jobs)
    print(f"待处理岗位总数: {total}，并发线程: {MAX_WORKERS}")

    failed_details = []
    success = 0
    done = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(call_llm, job) for job in jobs]

            for fu in as_completed(futures):
                res = fu.result()
                done += 1

                if not res["ok"]:
                    failed_details.append({
                        "id": res["id"],
                        "title": res["title"],
                        "stage": res.get("stage", "unknown"),
                        "error": res.get("error", ""),
                        "raw_text": (res.get("raw") or "")[:2000],
                        "time": datetime.now().isoformat(timespec="seconds")
                    })
                    print(f"❌ ID={res['id']} 失败: {res.get('error')}")
                    continue

                d = res["data"]
                try:
                    cursor.execute("""
                    UPDATE job_position
                    SET career_dir=%s,
                        job_level=%s,
                        skills=%s,
                        certificates=%s,
                        education_level=%s,
                        experience_years=%s,
                        soft_skills=%s,
                        job_tasks=%s,
                        domain_tags=%s,
                        open_knowledge=%s
                    WHERE id = %s
                    """, (
                        d.get("career_dir"),
                        d.get("job_level"),
                        safe_json(d.get("skills")),
                        safe_json(d.get("certificates")),
                        d.get("education_level"),
                        d.get("experience_years"),
                        safe_json(d.get("soft_skills")),
                        safe_json(d.get("job_tasks")),
                        safe_json(d.get("domain_tags")),
                        safe_json(d.get("open_knowledge")),
                        res["id"]
                    ))
                    conn.commit()
                    success += 1
                except Exception as e:
                    conn.rollback()
                    failed_details.append({
                        "id": res["id"],
                        "title": res["title"],
                        "stage": "db",
                        "error": f"{type(e).__name__}: {e}",
                        "raw_text": "",
                        "time": datetime.now().isoformat(timespec="seconds")
                    })
                    print(f"❌ ID={res['id']} 数据库失败: {e}")

                if done % 20 == 0 or done == total:
                    print(f"进度: {done}/{total}")

    finally:
        print("\n" + "=" * 50)
        print("任务结束")
        print(f"总数: {total}")
        print(f"成功: {success}")
        print(f"失败: {len(set(x['id'] for x in failed_details))}")
        print("=" * 50)

        write_failure_files(failed_details)
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()