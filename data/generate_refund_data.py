import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

records = []
categories = ["电子", "服装", "食品", "图书", "美妆"]
reasons = ["质量问题", "不喜欢", "发错货", "未收到", None] #none=dirty
for day_offset in range(7):
    date = (datetime(2025, 6, 1) + timedelta(days = day_offset)).strftime("%Y-%m-%d")
    for i in range(2000):
        if random.random() < 0.4:
            store_id = 1
        else:
            store_id = random.randint(2,50)

        if random.random() < 0.05:
            amount = None
        else:
            amount =random.randint(50, 500)
        
        r = random.random()
        if r < 0.1:
            refund_amount = None
        elif r < 0.2:
            refund_amount = -random.randint(1, 500)
        else:
            refund_amount = random.randint(0, amount or 100)
        
        category = random.choice(categories)
        refund_reason = random.choice(reasons)
        order_date = date
        order_id = i + day_offset * 2000
        user_id = random.randint(1,500)

        records.append({
            "store_id": store_id,
            "category": category,
            "amount": amount,
            "refund_amount": refund_amount,
            "refund_reason": refund_reason,
            "user_id": user_id,
            "date": order_date,
            "order_id": order_id
        })

df = pd.DataFrame(records)
df.to_csv("/Users/thatguy/de-venv/data/refund_orders.csv", index=False)
print(f"总行数: {len(df)}")
print(f"store_1 占比: {len(df[df['store_id']==1]) / len(df) * 100:.1f}%")
print(f"脏数据: refund为空={df['refund_amount'].isna().sum()}, refund负={ (df['refund_amount'] < 0).sum()}")

