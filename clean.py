import psycopg2
from psycopg2.extras import RealDictCursor, Json
import requests
import json
import re
import os

PAUSE_FILE = "pause.txt"

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="career_db",
    user="postgres",
    password="123456"
)
cursor = conn.cursor(cursor_factory=RealDictCursor)

cursor.execute("""
SELECT id, title, description
FROM job_position
WHERE career_dir IS NULL
   OR job_level IS NULL
   OR skills IS NULL
   OR tools IS NULL
   OR certificates IS NULL
   OR education_level IS NULL
   OR experience_years IS NULL
   OR soft_skills IS NULL
   OR job_tasks IS NULL
ORDER BY id ASC
""")

jobs = cursor.fetchall()
total = len(jobs)
print(f"待处理岗位总数: {total}")

failed_ids = []

def safe_json(v):
    if isinstance(v, list) and len(v) > 0:
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

    # 去掉 markdown 包裹
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

SYSTEM_RULE = """
你是岗位结构化信息提取助手。

【抽取规则】
1. 允许基于岗位名称和描述进行【合理推断】（例如根据“前端开发”推断career_dir为“前端”，或推断常识性基础技能），但严禁凭空捏造离谱的内容。若完全没有线索，填 null。
2. skills 必须是二维数组：
   - 或关系 → 分不同子数组
   - 且关系 → 同一子数组
3. job_level 必须判断：
   - 低：应届 / ≤1年
   - 中：2-5年
   - 高：≥5年 / 架构 / 负责人
4. soft_skills 只能从以下选择：
   ["沟通能力","团队合作能力","问题解决能力","持续学习能力","创新能力","抗压能力","某种语言能力"]
5. 其它选项含义：
   career_dir: 职业方向(比如前端、实施等)
   tools: 岗位要求的工具（比如Docker、Kubernetes等）
   certificates: 岗位要求的证书（比如PMP、AWS认证等）
   education_level: 学历要求（比如本科、硕士等）
   experience_years: 工作经验要求（比如3-5年）
   job_tasks: 岗位职责（比如设计系统架构、编写代码等）

最后必须严格输出且仅输出一个JSON对象，不要在JSON外面写任何解释文本。
【输出格式】
{
  "career_dir": null,
  "job_level": null,
  "skills": [],
  "tools": null,
  "certificates": null,
  "education_level": null,
  "experience_years": null,
  "soft_skills": [],
  "job_tasks": []
}
"""

for idx, job in enumerate(jobs, start=1):
    if os.path.exists(PAUSE_FILE):
        print("\n⚠️ 检测到暂停文件，程序已停止")
        break

    print(f"处理岗位 {idx}/{total}: {job['title']} (ID={job['id']})")

    user_text = f"""岗位名称: {job['title']}
岗位描述: {job['description']}"""

    # 用 /api/generate，避免 chat 的 thinking 干扰
    prompt = f"""{SYSTEM_RULE}

现在开始抽取，直接输出 JSON：
{user_text}
"""

    raw_text = ""
    try:
        resp = requests.post(
            "http://127.0.0.1:11434/api/generate",
            json={
                "model": "deepseek-r1:8b",
                "prompt": prompt,
                "stream": False,

                "options": {
                    "temperature": 0.1,
                }
            },
            timeout=500
        )
        print(f"--- [ID:{job['id']}] HTTP状态码: {resp.status_code}")
        resp.raise_for_status()

        print(f"--- [ID:{job['id']}] HTTP原始响应开始 ---")
        print(resp.text[:3000])
        print(f"--- [ID:{job['id']}] HTTP原始响应结束 ---")

        j = resp.json()
        raw_text = (j.get("response") or "").strip()

        print(f"--- [ID:{job['id']}] 模型response开始 ---")
        print(raw_text if raw_text else "<EMPTY>")
        print(f"--- [ID:{job['id']}] 模型response结束 ---")
        print(f"--- [ID:{job['id']}] 模型response repr ---")
        print(repr(raw_text))
        print()

        if not raw_text:
            raise ValueError("model response is empty")

        json_text = extract_first_json_object(raw_text)
        data = json.loads(json_text)

    except Exception as e:
        print(f"❌ 岗位 {job['id']} 处理失败!")
        print(f"错误原因: {type(e).__name__}: {e}")
        print(f"失败时原文repr: {repr(raw_text)}")
        failed_ids.append(job["id"])
        continue

    if not data.get("job_level"):
        data["job_level"] = infer_job_level(job.get("description", ""))

    if isinstance(data.get("skills"), list):
        fixed = []
        for item in data["skills"]:
            if isinstance(item, list):
                fixed.append(item)
            elif item is not None:
                fixed.append([item])
        data["skills"] = fixed
    else:
        data["skills"] = []

    if not isinstance(data.get("soft_skills"), list):
        data["soft_skills"] = []
    if not isinstance(data.get("job_tasks"), list):
        data["job_tasks"] = []

    try:
        cursor.execute("""
        UPDATE job_position
        SET career_dir=%s,
            job_level=%s,
            skills=%s,
            tools=%s,
            certificates=%s,
            education_level=%s,
            experience_years=%s,
            soft_skills=%s,
            job_tasks=%s
        WHERE id = %s
        """, (
            data.get("career_dir"),
            data.get("job_level"),
            safe_json(data.get("skills")),
            safe_json(data.get("tools")),
            safe_json(data.get("certificates")),
            data.get("education_level"),
            data.get("experience_years"),
            safe_json(data.get("soft_skills")),
            safe_json(data.get("job_tasks")),
            job["id"]
        ))
        conn.commit()
    except Exception as e:
        print(f"岗位 {job['id']} 数据库失败: {e}")
        conn.rollback()
        failed_ids.append(job["id"])

print("\n" + "=" * 50)
print("任务结束")
print(f"总数: {total}")
print(f"成功: {total - len(failed_ids)}")
print(f"失败: {len(failed_ids)}")
if failed_ids:
    print("失败ID：")
    print(failed_ids)
    with open("failed_ids.txt", "w", encoding="utf-8") as f:
        f.write(",".join(map(str, failed_ids)))
print("=" * 50)

cursor.close()
conn.close()