import re
import os
import xlrd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ── 数据库连接 ────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "career_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "123456"),
    )

# ── 读取XLS ───────────────────────────────────────────
def load_xls(path: str) -> list[dict]:
    wb = xlrd.open_workbook(path)
    ws = wb.sheet_by_index(0)
    headers = [str(ws.cell_value(0, col)).strip() for col in range(ws.ncols)]
    rows = []

    for row_idx in range(1, ws.nrows):
        row = {headers[col]: ws.cell_value(row_idx, col) for col in range(ws.ncols)}
        if any(str(v).strip() for v in row.values()):
            rows.append(row)

    wb.release_resources()
    print(f"读取完成：{len(rows)} 条")
    return rows

# ── 去重 ──────────────────────────────────────────────
KEY_FIELDS = [
    "岗位名称", "地址", "薪资范围", "公司名称",
    "所属行业", "公司规模", "公司类型", "岗位编码", "岗位详情",
]

def deduplicate(rows: list[dict]) -> list[dict]:
    seen = set()
    result = []
    for r in rows:
        key = "|".join(str(r.get(f) or "").strip() for f in KEY_FIELDS)
        if key in seen:
            continue
        seen.add(key)
        result.append(r)
    print(f"去重完成：{len(rows)} → {len(result)} 条")
    return result

# ── 薪资解析 ──────────────────────────────────────────
def parse_salary(raw: str) -> tuple[int, int]:
    raw = str(raw).strip()
    if not raw:
        return 0, 0
    if re.search(r'面议|面谈|面聊', raw):
        return -1, -1
    raw = re.sub(r'·\d+薪', '', raw).strip()
    if re.search(r'以下|以上|左右', raw):
        return 0, 0
    if re.search(r'元/天|元／天|元/日|元／日', raw):
        nums = re.findall(r'\d+', raw)
        if len(nums) >= 2:
            a, b = int(nums[0]), int(nums[1])
            return round(a * 21.75 / 1000), round(b * 21.75 / 1000)
        if len(nums) == 1:
            v = round(int(nums[0]) * 21.75 / 1000)
            return v, v
        return 0, 0
    if '万' in raw:
        nums = re.findall(r'\d+\.?\d*', raw)
        if len(nums) >= 2:
            return int(float(nums[0]) * 10), int(float(nums[1]) * 10)
        if len(nums) == 1:
            v = int(float(nums[0]) * 10)
            return v, v
    nums = re.findall(r'\d+', raw)
    if len(nums) >= 2:
        a, b = int(nums[0]), int(nums[1])
        if a > 1000:
            return a // 1000, b // 1000
        return a, b
    return 0, 0

# ── 过滤无效数据 ──────────────────────────────────────
def is_valid(record: dict) -> bool:
    title = str(record.get("岗位名称") or "").strip()
    detail = str(record.get("岗位详情") or "").strip()
    code = str(record.get("岗位编码") or "").strip()
    return bool(title and detail and code)

# ── 建表 ──────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS job_position (
    id            BIGSERIAL PRIMARY KEY,
    job_code      VARCHAR(100) NOT NULL,
    title         VARCHAR(200),
    address       VARCHAR(200),
    salary_raw    VARCHAR(100),
    salary_min    INT DEFAULT 0,
    salary_max    INT DEFAULT 0,
    company       VARCHAR(200),
    industry      VARCHAR(200),
    company_size  VARCHAR(100),
    company_type  VARCHAR(100),
    description   TEXT,
    company_intro TEXT,
    source_url    TEXT,
    update_date   VARCHAR(50),
    career_dir      VARCHAR(100),
    job_level       VARCHAR(200),
    skills          TEXT,
    tools           TEXT,
    certificates    TEXT,
    education_level VARCHAR(50),
    experience_years VARCHAR(50),
    soft_skills     TEXT,
    job_tasks       TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO job_position
  (job_code, title, address, salary_raw, salary_min, salary_max,
   company, industry, company_size, company_type,
   description, company_intro, source_url, update_date,
   career_dir, job_level,
   skills, tools, certificates,
   education_level, experience_years,
   soft_skills, job_tasks)
VALUES
  (%(job_code)s, %(title)s, %(address)s, %(salary_raw)s,
   %(salary_min)s, %(salary_max)s,
   %(company)s, %(industry)s, %(company_size)s, %(company_type)s,
   %(description)s, %(company_intro)s, %(source_url)s, %(update_date)s,
   %(career_dir)s, %(job_level)s,
   %(skills)s, %(tools)s, %(certificates)s,
   %(education_level)s, %(experience_years)s,
   %(soft_skills)s, %(job_tasks)s)
"""

CHECK_SQL = """
SELECT 1 FROM job_position
WHERE job_code = %(job_code)s
AND   title    = %(title)s
LIMIT 1
"""

# ── 获取字段值（兼容不同列名） ─────────────────────────
def get_field(record: dict, variants: list[str]) -> str:
    for name in variants:
        if name in record and str(record[name]).strip():
            return str(record[name]).strip()
    return ""

def get_source_url(record: dict) -> str:
    # 优先“岗位来源地址”，再兼容常见同义列
    candidates = [
        "岗位来源地址", "岗位来源链接", "岗位URL", "岗位链接",
        "岗位来源", "来源地址", "来源链接", "URL", "url"
    ]
    return get_field(record, candidates)

# ── 写入数据库 ────────────────────────────────────────
def write_to_db(records: list[dict]):
    conn = get_db()
    success = 0
    skip = 0
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
            for r in records:
                s_min, s_max = parse_salary(r.get("薪资范围", ""))
                source_url = get_source_url(r)
                data = {
                    "job_code": str(r.get("岗位编码") or "").strip(),
                    "title": str(r.get("岗位名称") or "").strip(),
                    "address": str(r.get("地址") or "").strip(),
                    "salary_raw": str(r.get("薪资范围") or "").strip(),
                    "salary_min": s_min,
                    "salary_max": s_max,
                    "company": str(r.get("公司名称") or "").strip(),
                    "industry": str(r.get("所属行业") or "").strip(),
                    "company_size": str(r.get("公司规模") or "").strip(),
                    "company_type": str(r.get("公司类型") or "").strip(),
                    "description": str(r.get("岗位详情") or "").strip(),
                    "company_intro": str(r.get("公司详情") or "").strip(),
                    "source_url": source_url,
                    "update_date": str(r.get("更新日期") or "").strip(),
                    "career_dir": None,
                    "job_level": None,
                    "skills": None,
                    "tools": None,
                    "certificates": None,
                    "education_level": None,
                    "experience_years": None,
                    "soft_skills": None,
                    "job_tasks": None
                }

                cur.execute(CHECK_SQL, {"job_code": data["job_code"], "title": data["title"]})
                if cur.fetchone():
                    skip += 1
                    continue

                try:
                    cur.execute(INSERT_SQL, data)
                    success += 1
                except Exception as e:
                    conn.rollback()
                    print(f"写入失败 [{data['job_code']}]: {e}")
                    skip += 1
            conn.commit()
    finally:
        conn.close()
    print(f"写入完成：成功 {success} 条，跳过 {skip} 条")

# ── 验证 ──────────────────────────────────────────────
def verify():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM job_position")
            total = cur.fetchone()["total"]
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN salary_min = -1 THEN 1 ELSE 0 END) AS negotiable,
                    SUM(CASE WHEN salary_min = 0 THEN 1 ELSE 0 END) AS unknown,
                    SUM(CASE WHEN salary_min > 0 THEN 1 ELSE 0 END) AS has_salary,
                    SUM(CASE WHEN source_url IS NOT NULL AND btrim(source_url) <> '' THEN 1 ELSE 0 END) AS has_source_url
                FROM job_position
            """)
            stats = cur.fetchone()
    finally:
        conn.close()
    print("\n── 数据验证 ───────────────")
    print("总条数：", total)
    print("有薪资：", stats["has_salary"])
    print("面议：", stats["negotiable"])
    print("未知：", stats["unknown"])
    print("有来源地址：", stats["has_source_url"])

# ── 主程序 ────────────────────────────────────────────
if __name__ == "__main__":
    rows = load_xls("jobs.xls")
    rows = deduplicate(rows)
    rows = [r for r in rows if is_valid(r)]
    print(f"过滤无效后：{len(rows)} 条")
    write_to_db(rows)
    verify()