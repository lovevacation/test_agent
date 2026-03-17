import psycopg2
from psycopg2.extras import RealDictCursor, Json
import requests
import json
import re
import os
from datetime import datetime

# ---------- 控制参数 ----------
PAUSE_FILE = "pause.txt"          # 手动创建这个文件即可暂停
FAILED_IDS_FILE = "failed_ids.txt"
ERROR_LOG_FILE = "error_details.log"

# ---------- 数据库连接 ----------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="career_db",
    user="postgres",
    password="123456"
)
cursor = conn.cursor(cursor_factory=RealDictCursor)

# ---------- 查询待处理岗位 ----------
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


# ---------- 日志 ----------
def log_error(job_id, title, reason, raw_text):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write(f"[{now}] job_id={job_id}, title={title}\n")
        f.write(f"reason: {reason}\n")
        f.write(f"raw_repr: {repr(raw_text)}\n")
        f.write(f"raw_text:\n{raw_text if raw_text is not None else 'None'}\n")


# ---------- JSON安全 ----------
def safe_json(v):
    if isinstance(v, list) and len(v) > 0:
        return Json(v, dumps=lambda x: json.dumps(x, ensure_ascii=False))
    return None


# ---------- job_level规则兜底 ----------
def infer_job_level(text):
    if not text:
        return None
    if re.search(r'应届|无经验|1年', text):
        return "低"
    if re.search(r'2年|3年|4年|5年', text):
        return "中"
    if re.search(r'5年|架构师|负责人|专家|高级', text):
        return "高"
    return None


# ---------- 清洗并解析模型输出 ----------
def extract_json_text(raw_text: str) -> str:
    """
    处理以下情况：
    1) 空字符串
    2) ```json ... ``` 包裹
    3) 前后有解释文字，只截取最外层 JSON
    """
    if raw_text is None:
        raise ValueError("AI returned None")

    text = raw_text.strip()
    if not text:
        raise ValueError("AI returned empty content")

    # 处理 markdown code block
    if text.startswith("```"):
        # 去掉首尾 ```
        text = re.sub(r"^```(?:json|JSON)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()

    # 如果不是以 { 开头，尝试抽取第一个完整 JSON 对象
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            text = text[start:end + 1].strip()

    return text


def parse_ai_json(raw_text: str) -> dict:
    text = extract_json_text(raw_text)
    return json.loads(text)


# ---------- Prompt ----------
SYSTEM_RULE = """
你是岗位结构化抽取助手。
必须严格输出JSON，不允许任何解释。

【强规则】
1. 不允许推测，没有写就填 null
2. skills 必须是二维数组：
   - 或关系 → 分不同子数组
   - 且关系 → 同一子数组
3. 不允许添加原文没有的技能
4. job_level 必须判断：
   - 低：应届 / ≤1年
   - 中：2-5年
   - 高：≥5年 / 架构 / 负责人
5. soft_skills 只能从以下选择：
   ["沟通能力","团队合作能力","问题解决能力","持续学习能力","创新能力","抗压能力","某种语言能力"]
6. 其它选项含义
career_dir:职业方向(比如后端)
tools:岗位要求的工具（比如Docker、Kubernetes等）
certificates:岗位要求的证书（比如PMP、AWS认证等）
education_level:学历要求（比如本科、硕士等、985优先等）
experience_years:工作经验要求（比如3-5年）
job_tasks:岗位职责（比如设计系统架构、编写代码等）

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


# ---------- 主循环 ----------
for idx, job in enumerate(jobs, start=1):
    # ✅ 暂停检测
    if os.path.exists(PAUSE_FILE):
        print("\n⚠️ 检测到暂停文件，程序已停止")
        break

    print(f"处理岗位 {idx}/{total}: {job['title']} (ID={job['id']})")

    prompt = f"""
岗位名称: {job.get('title', '')}
岗位描述: {job.get('description', '')}
"""

    content_str = ""

    # ---------- AI请求 ----------
    try:
        response = requests.post(
            "http://127.0.0.1:11434/api/chat",
            json={
                "model": "qwen3.5:9b",
                "messages": [
                    {"role": "system", "content": SYSTEM_RULE},
                    {"role": "user", "content": prompt}
                ],
                "format": "json",
                "options": {
                    "temperature": 0,
                    "num_predict": 800
                },
                "stream": False
            },
            timeout=1000
        )
        response.raise_for_status()

        resp_json = response.json()
        content_str = resp_json.get("message", {}).get("content", "")

        # ======= 打印原文（含repr） =======
        print(f"--- [ID:{job['id']}] AI 原始输出开始 ---")
        print(content_str if content_str else "<EMPTY>")
        print(f"--- [ID:{job['id']}] AI 原始输出结束 ---")
        print(f"--- [ID:{job['id']}] AI 原始输出repr ---")
        print(repr(content_str))
        print()

        data = parse_ai_json(content_str)

    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        print(f"❌ 岗位 {job['id']} 处理失败!")
        print(f"错误原因: {reason}")
        print(f"失败时原文repr: {repr(content_str)}")
        print(f"失败时的原文预览: {str(content_str)[:300]}")
        log_error(job["id"], job.get("title", ""), reason, content_str)
        failed_ids.append(job['id'])
        continue

    # ---------- 字段兜底 ----------
    if not data.get("job_level"):
        data["job_level"] = infer_job_level(job.get("description", ""))

    # ---------- skills结构校验：确保二维数组 ----------
    if isinstance(data.get("skills"), list):
        fixed = []
        for item in data["skills"]:
            if isinstance(item, list):
                fixed.append(item)
            elif item is None:
                continue
            else:
                fixed.append([item])
        data["skills"] = fixed
    else:
        data["skills"] = []

    # ---------- soft_skills/job_tasks 至少为 list ----------
    if not isinstance(data.get("soft_skills"), list):
        data["soft_skills"] = []
    if not isinstance(data.get("job_tasks"), list):
        data["job_tasks"] = []

    # ---------- 数据库更新 ----------
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
            job['id']
        ))

        conn.commit()

    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        print(f"❌ 岗位 {job['id']} 数据库失败: {reason}")
        conn.rollback()
        log_error(job["id"], job.get("title", ""), reason, json.dumps(data, ensure_ascii=False))
        failed_ids.append(job['id'])


# ---------- 输出总结 ----------
print("\n" + "=" * 50)
print("任务结束")
print(f"总数: {total}")
print(f"成功: {total - len(failed_ids)}")
print(f"失败: {len(failed_ids)}")

if failed_ids:
    print("失败ID：")
    print(failed_ids)
    with open(FAILED_IDS_FILE, "w", encoding="utf-8") as f:
        f.write(",".join(map(str, failed_ids)))
    print(f"失败ID已写入: {FAILED_IDS_FILE}")
    print(f"错误详情已写入: {ERROR_LOG_FILE}")

print("=" * 50)

cursor.close()
conn.close()