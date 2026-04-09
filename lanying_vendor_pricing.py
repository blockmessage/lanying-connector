def calc_adjusted_points(
    input_price,
    output_price,
    currency="USD",
    fx=7.0,  # ✅ 汇率参数（默认7）
    input_weight=0.75,
    output_weight=0.25,
    base_price=0.00075,
    fixed=0.22222222,
    digits=2,
):
    input_price = float(input_price)
    output_price = float(output_price)
    currency = str(currency or "USD").upper()

    # 1️⃣ 单位统一：1M → 1k
    if currency == "USD":
        input_price = input_price / 1000.0
        output_price = output_price / 1000.0
        fx_factor = fx      # USD 用汇率
    elif currency == "CNY":
        fx_factor = 1.0     # CNY 不用汇率
    else:
        raise ValueError("currency must be USD or CNY")

    # 2️⃣ 合成 price
    price = input_price * input_weight + output_price * output_weight

    # 3️⃣ 核心公式（统一）
    adjusted = fixed + (price / base_price) * (fx_factor / 9)

    return round(adjusted, digits)


def apply_model_pricing(config, input_price, output_price, currency="USD", digits=2):
    config["input_price"] = float(input_price)
    config["output_price"] = float(output_price)
    config["currency"] = str(currency or "USD").upper()
    config["quota"] = calc_adjusted_points(
        input_price=input_price,
        output_price=output_price,
        currency=currency,
        digits=digits,
    )
    return config


def compare_model_quota(config, digits=2, diff_digits=8):
    current_quota = float(config.get("quota", 0))
    input_price = config.get("input_price")
    output_price = config.get("output_price")
    currency = config.get("currency", "USD")
    if input_price is None or output_price is None:
        raise ValueError("model config must include input_price and output_price")

    calculated_quota = calc_adjusted_points(
        input_price=input_price,
        output_price=output_price,
        currency=currency,
        digits=digits,
    )
    diff = round(current_quota - calculated_quota, diff_digits)
    return {
        "model": config.get("model", ""),
        "service": config.get("service", ""),
        "currency": str(currency or "USD").upper(),
        "input_price": float(input_price),
        "output_price": float(output_price),
        "quota": current_quota,
        "calculated_quota": calculated_quota,
        "quota_diff": diff,
        "is_matched": diff == 0,
    }


def compare_model_quotas(configs, digits=2, diff_digits=8):
    results = []
    for config in configs:
        if not isinstance(config, dict):
            continue
        if "input_price" not in config or "output_price" not in config:
            continue
        results.append(compare_model_quota(config, digits=digits, diff_digits=diff_digits))
    return results


def format_quota_diff_report(configs, digits=2, diff_digits=8):
    items = compare_model_quotas(configs, digits=digits, diff_digits=diff_digits)
    matched = [item for item in items if item.get("is_matched") is True]
    mismatched = [item for item in items if item.get("is_matched") is not True]
    lines = []
    for item in items:
        lines.append(
            "{model} | quota={quota} | calculated_quota={calculated_quota} | quota_diff={quota_diff} | matched={is_matched}".format(
                model=item.get("model", ""),
                quota=item.get("quota"),
                calculated_quota=item.get("calculated_quota"),
                quota_diff=item.get("quota_diff"),
                is_matched=item.get("is_matched"),
            )
        )
    return {
        "total": len(items),
        "matched": len(matched),
        "mismatched": len(mismatched),
        "lines": lines,
        "items": items,
        "mismatched_items": mismatched,
    }

def show_report():
    import lanying_vendor_openai
    import lanying_vendor_aliyun
    report = format_quota_diff_report(
        lanying_vendor_openai.model_configs() + lanying_vendor_aliyun.model_configs()
    )
    for line in report["lines"]:
        print(line)
