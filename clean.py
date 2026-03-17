import os
import re
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from concurrent.futures import ThreadPoolExecutor, as_completed

PAUSE_FILE = "pause.txt"
OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
MODEL_NAME = "deepseek-r1:8b"
MAX_WORKERS = int(os.getenv("CLEAN_MAX_WORKERS", "3"))  # 建议 4~8 先试

SOFT_SKILL_WHITELIST = {
    "沟通能力", "团队合作能力", "问题解决能力",
    "持续学习能力", "创新能力", "抗压能力", "某种语言能力"
}

SYSTEM_RULE = """
你是岗位结构化信息提取助手。

【抽取规则】
1. 允许基于岗位名称和描述进行合理推断，但严禁捏造无关内容。
2. skills 必须是二维数组，并严格遵循：
   - 子数组内：OR关系（会其中一种或多种）
   - 子数组间：AND关系（与其它子数组共同构成完整技能要求）
3. job_level 必须判断：
   - 低：应届 / ≤1年
   - 中：2-5年
   - 高：≥5年 / 架构 / 负责人
4. soft_skills 只能从以下选择：
   ["沟通能力","团队合作能力","问题解决能力","持续学习能力","创新能力","抗压能力","某种语言能力"]
5. 对“小众/跨领域岗位”额外抽取：
   - domain_tags: 领域标签数组（如["土木工程","材料","生物医药","金融","制药"]）
   - open_knowledge: 难标准化的开放知识点数组（如["混凝土耐久性优化设计方法","细胞培养基础知识"]）
6. 字段含义：
   career_dir: 职业方向
   certificates: 岗位要求证书
   education_level: 学历要求
   experience_years: 工作经验要求
   job_tasks: 岗位职责

最后必须严格输出且仅输出一个JSON对象，不要在JSON外写解释文本。

【输出格式】
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
    if re.search(r'应届|无经验|1年', text):
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

    depth, end = 0, -1
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        raise ValueError("no complete JSON object found")
    return s[start:end + 1]

def normalize_skills(skills):
    if not isinstance(skills, list):
        return []
    fixed = []
    for item in skills:
        if isinstance(item, list):
            group = [str(x).strip() for x in item if str(x).strip()]
        elif item is not None:
            group = [str(item).strip()]
        else:
            group = []
        group = list(dict.fromkeys([x for x in group if x]))
        if group:
            fixed.append(group)
    return fixed

def normalize_str_list(v):
    if not isinstance(v, list):
        return []
    return list(dict.fromkeys([str(x).strip() for x in v if str(x).strip()]))

def call_llm(job):
    if os.path.exists(PAUSE_FILE):
        return {"id": job["id"], "ok": False, "error": "paused", "raw": ""}

    user_text = f"岗位名称: {job['title']}\n岗位描述: {job['description']}"
    prompt = f"{SYSTEM_RULE}\n\n现在开始抽取，直接输出 JSON：\n{user_text}"

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
        data["certificates"] = normalize_str_list(data.get("certificates"))
        data["job_tasks"] = normalize_str_list(data.get("job_tasks"))
        data["domain_tags"] = normalize_str_list(data.get("domain_tags"))
        data["open_knowledge"] = normalize_str_list(data.get("open_knowledge"))

        soft = normalize_str_list(data.get("soft_skills"))
        data["soft_skills"] = [x for x in soft if x in SOFT_SKILL_WHITELIST]

        return {"id": job["id"], "ok": True, "data": data}
    except Exception as e:
        return {"id": job["id"], "ok": False, "error": f"{type(e).__name__}: {e}", "raw": raw_text}

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
    print(f"待处理岗位总数: {total}, 并发线程: {MAX_WORKERS}")

    failed_ids = []
    done = 0

    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for job in jobs:
            futures.append(ex.submit(call_llm, job))

        for fu in as_completed(futures):
            res = fu.result()
            done += 1

            if not res["ok"]:
                if res.get("error") != "paused":
                    print(f"❌ ID={res['id']} 失败: {res.get('error')}")
                    failed_ids.append(res["id"])
                else:
                    print("⚠️ 检测到暂停文件，停止提交新任务")
                continue

            data = res["data"]
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
                    data.get("career_dir"),
                    data.get("job_level"),
                    safe_json(data.get("skills")),
                    safe_json(data.get("certificates")),
                    data.get("education_level"),
                    data.get("experience_years"),
                    safe_json(data.get("soft_skills")),
                    safe_json(data.get("job_tasks")),
                    safe_json(data.get("domain_tags")),
                    safe_json(data.get("open_knowledge")),
                    res["id"]
                ))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"❌ ID={res['id']} 数据库失败: {e}")
                failed_ids.append(res["id"])

            if done % 20 == 0 or done == total:
                print(f"进度: {done}/{total}")

    print("\n" + "=" * 50)
    print("任务结束")
    print(f"总数: {total}")
    print(f"成功: {total - len(failed_ids)}")
    print(f"失败: {len(failed_ids)}")
    if failed_ids:
        print("失败ID：", failed_ids)
        with open("failed_ids.txt", "w", encoding="utf-8") as f:
            f.write(",".join(map(str, failed_ids)))
    print("=" * 50)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()