import re
import xlrd


def parse_salary(raw: str) -> tuple[int, int]:
    raw = str(raw).strip()
    if not raw:
        return 0, 0

    # 面议/面谈
    if re.search(r'面议|面谈|面聊', raw):
        return -1, -1

    # 去掉·13薪 ·14薪等后缀
    raw = re.sub(r'·\d+薪', '', raw).strip()

    # 以下/以上/左右 等模糊描述
    if re.search(r'以下|以上|左右', raw):
        return 0, 0

    # 日薪（元/天）→ 换算成月薪k（× 21.75 ÷ 1000）
    if re.search(r'元/天|元／天|元/日|元／日', raw):
        nums = re.findall(r'\d+', raw)
        if len(nums) >= 2:
            a, b = int(nums[0]), int(nums[1])
            return round(a * 21.75 / 1000), round(b * 21.75 / 1000)
        if len(nums) == 1:
            v = round(int(nums[0]) * 21.75 / 1000)
            return v, v
        return 0, 0

    # 万元
    if '万' in raw:
        nums = re.findall(r'\d+\.?\d*', raw)
        if len(nums) >= 2:
            return int(float(nums[0]) * 10), int(float(nums[1]) * 10)
        if len(nums) == 1:
            v = int(float(nums[0]) * 10)
            return v, v

    # 元单位
    nums = re.findall(r'\d+', raw)
    if len(nums) >= 2:
        a, b = int(nums[0]), int(nums[1])
        if a > 1000:
            return a // 1000, b // 1000
        return a, b

    return 0, 0


def run_tests():
    test_cases = [
        ("3000-4000元",         (3, 4)),
        ("120-150元/天",        (3, 3)),
        ("150-200元/天",        (3, 4)),
        ("10-20元/天",          (0, 0)),
        ("7000-10000元·13薪",   (7, 10)),
        ("7000-12000元",        (7, 12)),
        ("6000-8000元·14薪",    (6, 8)),
        ("1-1.3万",             (10, 13)),
        ("1-2万",               (10, 20)),
        ("1.2-2万",             (12, 20)),
        ("1.5-3万",             (15, 30)),
        ("1000元以下",          (0, 0)),
        ("1000元以下·13薪",     (0, 0)),
        ("面议",                (-1, -1)),
        ("",                    (0, 0)),
    ]

    print("验证结果：")
    all_pass = True
    for raw, expected in test_cases:
        result = parse_salary(raw)
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_pass = False
        print(f"  {status}  {raw:25s} → {result}  期望{expected}")

    print(f"\n{'全部通过' if all_pass else '有失败项，需要修正'}")
    return all_pass


def scan_parse_result(path: str):
    wb = xlrd.open_workbook(path)
    ws = wb.sheet_by_index(0)
    headers = [str(ws.cell_value(0, col)).strip() for col in range(ws.ncols)]
    rows = []
    for row_idx in range(1, ws.nrows):
        row = {headers[col]: ws.cell_value(row_idx, col) for col in range(ws.ncols)}
        if any(str(v).strip() for v in row.values()):
            rows.append(row)
    wb.release_resources()

    total        = len(rows)
    ok           = 0
    negotiable   = 0
    known_zero   = 0   # 主动返回(0,0)的已知情况
    truly_failed = []  # 真正没覆盖到的格式

    KNOWN_ZERO_PATTERNS = [
        r'以下|以上|左右',
        r'元/天|元／天|元/日|元／日',
    ]

    for r in rows:
        raw = str(r.get("薪资范围") or "").strip()
        s_min, s_max = parse_salary(raw)

        if not raw:
            continue
        elif s_min == -1:
            negotiable += 1
        elif s_min == 0 and s_max == 0:
            # 判断是主动返回0还是真的没解析到
            raw_stripped = re.sub(r'·\d+薪', '', raw).strip()
            if any(re.search(p, raw_stripped) for p in KNOWN_ZERO_PATTERNS):
                known_zero += 1
            else:
                truly_failed.append(raw)
        else:
            ok += 1

    print(f"\n真实数据解析结果（共{total}条）：")
    print(f"  正常解析：  {ok} 条")
    print(f"  面议：      {negotiable} 条")
    print(f"  已知零值：  {known_zero} 条（日薪/模糊描述，主动置0）")
    print(f"  真正失败：  {len(truly_failed)} 条")

    if truly_failed:
        from collections import Counter
        top = Counter(truly_failed).most_common(20)
        print(f"\n  未覆盖格式（Top20）：")
        for val, cnt in top:
            print(f"    {cnt:4d} 次  {repr(val)}")
    else:
        print(f"\n  所有格式已全部覆盖，parse_salary 可以集成到清洗脚本了")


if __name__ == "__main__":
    passed = run_tests()
    if passed:
        scan_parse_result("jobs.xls")
    else:
        print("\n测试未通过，请先修正 parse_salary 再扫描真实数据")