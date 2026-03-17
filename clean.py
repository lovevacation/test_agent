import psycopg2
from psycopg2.extras import RealDictCursor, Json
import requests
import json
import re
import sys

# ---------- 控制参数 ----------
PAUSE_FILE = "pause.txt"   # 手动创建这个文件即可暂停

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


# ---------- JSON安全 ----------
def safe_json(v):
    if isinstance(v, list) and len(v) > 0:
        return Json(v, dumps=lambda x: json.dumps(x, ensure_ascii=False))
    return None


# ---------- job_level规则兜底 ----------
def infer_job_level(text):
    if re.search(r'应届|无经验|1年', text):
        return "低"
    if re.search(r'2年|3年|4年|5年', text):
        return "中"
    if re.search(r'5年|架构师|负责人|专家|高级', text):
        return "高"
    return None


# ---------- Prompt（核心优化） ----------
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
6.其它选项含义
career_dir:职业方向(比如后端)
tools:岗位要求的工具（比如Docker、Kubernetes等）
certificates:岗位要求的证书（比如PMP、AWS认证等）
education_level:学历要求（比如本科、硕士等、985优先等）
experience_years:工作经验要求（比如3-5年）
job_tasks:岗位职责（比如设计系统架构、编写代码等）
【输出格式】
{
  "career_dir": ,
  "job_level": ,
  "skills": [],
  "tools": ,
  "certificates": ,
  "education_level": ,
  "experience_years": ,
  "soft_skills": [],
  "job_tasks": []
}
"""


# ---------- 主循环 ----------
for idx, job in enumerate(jobs, start=1):

    # ✅ 暂停检测
    try:
        with open(PAUSE_FILE, "r"):
            print("\n⚠️ 检测到暂停文件，程序已停止")
            break
    except FileNotFoundError:
        pass

    print(f"处理岗位 {idx}/{total}: {job['title']} (ID={job['id']})")

    prompt = f"""
岗位名称: {job['title']}
岗位描述: {job['description']}
"""

    # ---------- AI请求 ----------
    # ---------- AI请求 ----------
    try:
        response = requests.post(
            "http://127.0.0.1:11434/api/chat",
            json={
                "model": "qwen3:14b",
                "messages": [
                    {"role": "system", "content": SYSTEM_RULE},
                    {"role": "user", "content": prompt}
                ],
                "format": "json",
                "options": {
                    "temperature": 0,
                    "num_predict": 800  # ✅ 建议调大到 800，防止内容被砍
                },
                "stream": False
            },
            timeout=300
        )
        resp_json = response.json()
        content_str = resp_json.get("message", {}).get("content", "").strip()

        # ======= 新增：输出原文到控制台 =======
        print(f"--- [ID:{job['id']}] AI 原始输出开始 ---")
        print(content_str)
        print(f"--- [ID:{job['id']}] AI 原始输出结束 ---\n")
        # ===================================

        data = json.loads(content_str)

    except Exception as e:
        print(f"❌ 岗位 {job['id']} 处理失败!")
        print(f"错误原因: {e}")
        # 如果解析失败，把断掉的内容打印出来方便排查
        if 'content_str' in locals():
            print(f"失败时的原文预览: {content_str[:100]}...")
        failed_ids.append(job['id'])
        continue

    # ---------- job_level兜底 ----------
    if not data.get("job_level"):
        data["job_level"] = infer_job_level(job["description"])

    # ---------- skills结构校验 ----------
    if isinstance(data.get("skills"), list):
        # 确保二维数组
        fixed = []
        for item in data["skills"]:
            if isinstance(item, list):
                fixed.append(item)
            else:
                fixed.append([item])
        data["skills"] = fixed

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
        print(f"岗位 {job['id']} 数据库失败: {e}")
        conn.rollback()
        failed_ids.append(job['id'])


# ---------- 输出总结 ----------
print("\n" + "=" * 50)
print(f"任务结束")
print(f"总数: {total}")
print(f"成功: {total - len(failed_ids)}")
print(f"失败: {len(failed_ids)}")

if failed_ids:
    print("失败ID：")
    print(failed_ids)

    # ✅ 写入文件（暂停时也能看）
    with open("failed_ids.txt", "w") as f:
        f.write(",".join(map(str, failed_ids)))

print("=" * 50)

cursor.close()
conn.close()